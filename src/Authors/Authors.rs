use clap::{arg, Parser};
use colored::Colorize;
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Write};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{env, fs, io, path, thread};
use walkdir::WalkDir;

use openalex_collaboration_crawler::graph_utils::{
    get_num_threads, merge_files, process_directories, process_single_author_file, read_lines,
};

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(
        short = 'f',
        long,
        help = "Two letter country code. Authors will be kept only if at some point in time have been affiliated to the given country.",
        value_name = "COUNTRY_CODE"
    )]
    country_code_filter: Option<String>,

    #[arg(
        short = 'i',
        long,
        help = "OpenAlex AWS snapshot directory",
        value_name = "DIR"
    )]
    openalex_input_dir: String,

    #[arg(
        short = 'o',
        long,
        help = "Output file name",
        default_value = "authors.jsonl"
    )]
    output_file_name: Option<String>,
}

fn main() {
    let args = Args::parse();

    /*
    Sequence of execution: extractor + compressor -> filter
    */

    let output_file_name_compress = if args.country_code_filter.is_none() {
        args.output_file_name
            .clone()
            .unwrap_or_else(|| "authors_compressed.jsonl".to_string())
    } else {
        "authors_compressed.jsonl".to_string()
    };

    let input_dir = args.openalex_input_dir;

    println!("[i] {}", "Starting extractor phase".blue());
    println!(
        "[i] {}{}",
        "Openalex AWS snapshot directory: ".blue(),
        input_dir.yellow()
    );

    let paths = process_directories(input_dir);
    let num_cores_avail = std::cmp::min(paths.len() as u64, get_num_threads());

    let progress = ProgressBar::new(paths.len() as u64);
    progress.set_style(
        ProgressStyle::with_template("[i] Extracted files: [{wide_bar:.cyan/blue}] {percent}%")
            .unwrap()
            .progress_chars("#>-"),
    );

    let bar = Arc::new(Mutex::new(progress));

    let mut handles = vec![];

    for i in 0..num_cores_avail {
        let progress_bar = Arc::clone(&bar);

        let handle = thread::spawn({
            let slice = paths.clone();
            let thread_id = i;
            move || {
                let mut _output_file = File::create(
                    "/tmp/extractor.part.".to_string() + thread_id.to_string().as_str(),
                )
                .unwrap();

                let slice_size = slice.len() as u64 / num_cores_avail;
                let lowerbound = thread_id * slice_size;
                let upperbound = match i == num_cores_avail - 1 {
                    true => slice.len() as u64,
                    false => (thread_id + 1) * slice_size,
                };

                for i in lowerbound..upperbound {
                    let path = slice[i as usize].to_string();
                    process_single_author_file(path, &mut _output_file);
                    let pb = progress_bar.lock().unwrap();
                    pb.inc(1);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Failed to join child thread");
    }

    println!("[i] {}", "Completed extractor phase".green());
    println!("[i] {}", "Merging extracted data".yellow());

    let files_to_merge: Vec<String> = (0..num_cores_avail)
        .map(|i| format!("/tmp/extractor.part.{i}"))
        .collect();

    merge_files(&files_to_merge, "/tmp/authors.jsonl").expect("Unable to merge files");

    let mut affiliation_dataset: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    {
        let bar = ProgressBar::new(
            File::open("/tmp/authors.jsonl")
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
        );

        bar.set_style(
            ProgressStyle::with_template(
                "[i] Loading data from temp file [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        for line in read_lines("/tmp/authors.jsonl").unwrap() {
            let line_unwrapped = line.unwrap();
            bar.inc((line_unwrapped.chars().count() + 1) as u64);
            let object: Value = serde_json::from_str(line_unwrapped.as_str()).unwrap();
            let id = object["id"].to_string();

            // Add entry to map if not found
            let aff_map = affiliation_dataset
                .entry(id.clone())
                .or_insert(HashMap::new());

            for entry in object["affs"].as_array().unwrap() {
                for (key, value) in entry.as_object().unwrap() {
                    let aff_vec = aff_map.entry(key.clone()).or_insert(Vec::new());
                    if !aff_vec.contains(&value.to_string()) {
                        aff_vec.push(value.to_string());
                    }
                }
            }
        }
    }

    {
        println!("[i] {}", "Starting compress phase".blue());
        let bar = ProgressBar::new(affiliation_dataset.len() as u64);

        bar.set_style(
            ProgressStyle::with_template("[i] Saving data [{wide_bar:.cyan/blue}] {percent}%")
                .unwrap()
                .progress_chars("#>-"),
        );
        let mut output_file =
            File::create(&output_file_name_compress).expect("Unable to create file");

        for (openalex_id, country_affiliations) in &affiliation_dataset {
            bar.inc(1);

            // "year" : [ "country1" , "country2" , ... ]
            let affs_json: Map<String, Value> = country_affiliations
                .iter()
                .map(|(year, country_code)| (year.clone(), json!(country_code)))
                .collect();

            let record = json!({
                "id": openalex_id,
                "affs": affs_json
            });

            writeln!(output_file, "{}", record.to_string()).unwrap();
        }

        println!("[i] {}", "Completed compress stage".green());
    }

    if args.country_code_filter.is_some() {
        let output_file_name_filter: String = args
            .output_file_name
            .unwrap_or_else(|| "authors_filtered.jsonl".to_string());

        let mut output_file = File::create(output_file_name_filter).unwrap();
        let formatted_country = format!("\"{}\"", args.country_code_filter.unwrap());

        let bar = ProgressBar::new(
            File::open(&output_file_name_compress)
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
        );

        bar.set_style(
            ProgressStyle::with_template(
                "[i] Saving filtered data [{wide_bar:.cyan/blue}] {percent}%",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        match read_lines(&output_file_name_compress) {
            Ok(lines) => {
                for line in lines {
                    let unwrapped = line.unwrap();
                    bar.inc(unwrapped.chars().count() as u64);
                    if unwrapped.contains(formatted_country.as_str()) {
                        match writeln!(output_file, "{}", unwrapped) {
                            Ok(_) => (),
                            Err(err) => println!("Failed to write to file: {}", err),
                        }
                    }
                }
            }
            Err(why) => {
                println!("IO Error: {}", why);
                exit(-2)
            }
        }
    }
}
