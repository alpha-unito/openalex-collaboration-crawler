use clap::{arg, Parser};
use colored::Colorize;
use openalex_graph_gen::graph_utils::{
    get_num_threads, merge_files, process_directories, process_single_author_file, read_lines,
};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(long, help = "Extract from compressed files")]
    extract: bool,

    #[arg(long, help = "Compress to single unique entries")]
    compress: bool,

    #[arg(long, help = "Filter the dataset")]
    filter: bool,

    #[arg(long, help = "Country code filter", value_name = "COUNTRY_CODE")]
    country_code: Option<String>,

    #[arg(long, help = "OpenAlex authors source directory", value_name = "DIR")]
    input_dir: Option<String>,

    #[arg(
        long,
        help = "Output file name",
        value_name = "FILE.jsonl",
        default_value = "authors.jsonl"
    )]
    output: Option<String>,
}

fn main() {
    let args = Args::parse();

    let mut num_cores_avail = get_num_threads();

    /*
    Sequence of execution: extractor -> compressor -> filter
    */
    let output_file_name_extract: String = "authors.jsonl".to_string();
    let outputfile_name_compress: String = "authors_compressed.jsonl".to_string();
    let output_file_name_filter: String = "authors_filtered.jsonl".to_string();

    if args.extract {
        let input_dir = args.input_dir.unwrap();
        println!("[i] {}", "Starting extractor phase".blue());
        println!("[i] {}{}", "source file: ".blue(), input_dir.yellow());

        let paths = process_directories(input_dir);
        if (paths.len() as u64) < num_cores_avail {
            num_cores_avail = paths.len() as u64;
        }

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
                        "extractor.part.".to_string() + thread_id.to_string().as_str(),
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

        let mut files_to_merge: Vec<String> = Vec::new();
        for i in 0..num_cores_avail {
            files_to_merge.push("extractor.part.".to_string() + i.to_string().as_str());
        }

        merge_files(&files_to_merge, &output_file_name_extract).expect("Unable to merge files");

        println!("[i] {}", "Finished merging extracted data".green());
    }

    if args.compress {
        println!("[i] {}", "Starting compress phase".blue());

        let mut dataset: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

        let bar = ProgressBar::new(
            File::open(&output_file_name_extract)
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
        );

        bar.set_style(
            ProgressStyle::with_template(
                "[i] Loading data [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        for line in read_lines(&output_file_name_extract).unwrap() {
            let line_unwrapped = line.unwrap();
            bar.inc((line_unwrapped.chars().count() + 1) as u64);
            let object: Value = serde_json::from_str(line_unwrapped.as_str()).unwrap();
            let id = object["id"].to_string();

            // Add entry to map if not found
            let aff_map = dataset.entry(id.clone()).or_insert(HashMap::new());

            for entry in object["affs"].as_array().unwrap() {
                for (key, value) in entry.as_object().unwrap() {
                    let aff_vec = aff_map.entry(key.clone()).or_insert(Vec::new());
                    if !aff_vec.contains(&value.to_string()) {
                        aff_vec.push(value.to_string());
                    }
                }
            }
        }

        println!("[i] {}", "Finished loading data".green());
        {
            let bar = ProgressBar::new(dataset.len() as u64);

            bar.set_style(
                ProgressStyle::with_template("[i] Saving data [{wide_bar:.cyan/blue}] {percent}%")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            let mut output_file =
                File::create(&outputfile_name_compress).expect("Unable to create file");
            for (key, value) in &dataset {
                bar.inc(1);

                write!(output_file, "{{ \"id\": {}, \"affs\": {{", key).unwrap();
                for (year_index, (year, affs)) in value.iter().enumerate() {
                    write!(output_file, "\"{}\": [", year).unwrap();
                    for (affiliation_index, aff) in affs.iter().enumerate() {
                        write!(output_file, "{} ", aff).unwrap();
                        if affiliation_index != affs.len() - 1 {
                            write!(output_file, ",").unwrap();
                        }
                    }
                    write!(output_file, "]").unwrap();
                    if year_index != value.len() - 1 {
                        write!(output_file, ",").unwrap();
                    }
                }

                writeln!(output_file, "}} }}").unwrap();
            }
        }

        println!("[i] {}", "Completed compress stage".green());
    }

    if args.filter {
        let mut output_file = File::create(output_file_name_filter).unwrap();
        let formatted_country: String =
            "\"".to_string() + args.country_code.unwrap().as_str() + "\"";

        let bar = ProgressBar::new(
            File::open(&outputfile_name_compress)
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

        match read_lines(&outputfile_name_compress) {
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
