use clap::{arg, Parser};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use openalex_collaboration_crawler::graph_utils::{
    extract_topics, get_num_threads, load_authors_from_file, merge_files, process_directories,
    process_single_work_file,
};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, Seek, SeekFrom};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{io, thread};

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(
        long,
        help = "If set before filtering paper, the extraction phase will take place."
    )]
    extract: bool,

    #[arg(long, help = "If set the filtering phase will take place.")]
    filter: bool,

    #[arg(
        long,
        help = "Skip merging temporary extractor phase files, allowing for streaming operations \
        between the two phases. The merge to a sinle file will still occur at the end of filter phase"
    )]
    skip_merge: bool,

    #[arg(
        long,
        help = "Source file obtained from authors steps. If set will filter for authors contained inside that file.",
        value_name = "AUTHOR_FILE_PATH"
    )]
    author_filter: Option<String>,

    #[arg(
        long,
        help = "Country code of the affiliation of an author, for a given year.",
        value_name = "COUNTRY_CODE"
    )]
    affiliate: Option<String>,

    #[arg(
        long,
        help = "String containing a topic. If set will filter papers against this given topic.",
        value_name = "\"TOPIC\""
    )]
    topic_name: Option<String>,

    #[arg(
        long,
        help = "Path of the directory op openal;ex snapshot containing compressed papers entries",
        value_name = "PATH"
    )]
    input_directory: Option<String>,
}

fn main() {
    let _args = Args::parse();

    if _args.author_filter.is_none()
        && _args.affiliate.is_none()
        && _args.topic_name.is_none()
        && _args.input_directory.is_none()
    {
        println!("Error: no command given!");
        exit(-1);
    }

    let mut num_cores_avail = get_num_threads();

    let author_filter_file_path = _args.author_filter.unwrap_or_else(|| String::new());

    let author_filter_map = match !author_filter_file_path.is_empty() {
        true => {
            println!(
                "[i] {} {}",
                "Using author filter:".blue(),
                author_filter_file_path.yellow()
            );

            load_authors_from_file(&author_filter_file_path)
        }
        false => HashMap::new(),
    };

    let mut author_filter: Vec<String> = author_filter_map.keys().cloned().collect();
    author_filter.sort();

    if _args.extract {
        println!("[i] {}", "Starting extractor phase".blue());

        let output_file = "papers.jsonl";
        let input_files = process_directories(_args.input_directory.clone().unwrap());

        if (input_files.len() as u64) < num_cores_avail {
            num_cores_avail = input_files.len() as u64;
            println!(
                "Warning: less files than cores. running with {} threads",
                num_cores_avail
            );
        }

        let _bar = ProgressBar::new(input_files.len() as u64);
        _bar.set_style(
            ProgressStyle::with_template("[i] Extracted files: [{wide_bar:.cyan/blue}] {percent}%")
                .unwrap()
                .progress_chars("#>-"),
        );

        let bar = Arc::new(Mutex::new(_bar));

        let mut handles = vec![];

        for i in 0..num_cores_avail {
            let progress_bar = Arc::clone(&bar);

            let handle = thread::spawn({
                let slice = input_files.clone();
                let thread_id = i;
                let _authors = author_filter.clone();
                move || {
                    let mut _output_file = File::create(
                        "work-extractor.part.".to_string() + thread_id.to_string().as_str(),
                    )
                    .unwrap();

                    let slice_size = slice.len() as u64 / num_cores_avail;
                    let lower_bound = thread_id * slice_size;
                    let upperbound = match i == num_cores_avail - 1 {
                        true => slice.len() as u64,
                        false => (thread_id + 1) * slice_size,
                    };

                    for i in lower_bound..upperbound {
                        let path = slice[i as usize].to_string();
                        process_single_work_file(path, &mut _output_file, &_authors);
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

        if !_args.skip_merge {
            println!("[i] {}", "Starting extractor-merge phase".blue());

            let mut files_to_merge: Vec<String> = Vec::new();
            for i in 0..num_cores_avail {
                files_to_merge.push("work-extractor.part.".to_string() + i.to_string().as_str());
            }

            merge_files(&files_to_merge, &output_file).expect("Unable to merge files");
            println!("[i] {}", "Completed extractor-merge phase".green());
        }
    }

    if _args.filter {
        // topic Filter phase
        let file_size = match _args.skip_merge {
            true => {
                let mut file_size: u64 = 0;
                for i in 0..num_cores_avail {
                    file_size +=
                        File::open("work-extractor.part.".to_string() + i.to_string().as_str())
                            .unwrap()
                            .metadata()
                            .unwrap()
                            .len();
                }
                file_size
            }
            false => File::open("papers.jsonl")
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
        };

        println!("[i] {}", "Starting filter phase".blue());
        let _bar = ProgressBar::new(file_size);
        _bar.set_style(
            ProgressStyle::with_template(
                "[i] Filtering papers: [{wide_bar:.cyan/blue}] {percent}%",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        let bar = Arc::new(Mutex::new(_bar));

        num_cores_avail = get_num_threads();

        let single_thread_read_size = file_size / num_cores_avail;
        let mut handles = vec![];

        for i in 0..num_cores_avail {
            let handle = thread::spawn({
                let thread_topic_filter_keyword: String = _args.topic_name.clone().unwrap();
                let _authors_filter: Vec<String> = author_filter.clone();
                let progress_bar = Arc::clone(&bar);
                let _author_filter_map = author_filter_map.clone();
                let affiliation = _args.affiliate.clone().unwrap();

                let mut _output_file =
                    File::create("dataset.part.".to_string() + i.to_string().as_str()).unwrap();

                let in_filename = match _args.skip_merge {
                    false => "papers.jsonl".to_string(),
                    true => "work-extractor.part.".to_string() + i.to_string().as_str(),
                };

                let input_file_ptr = File::open(in_filename).unwrap();
                let input_file_metadata = input_file_ptr.metadata().unwrap();
                let mut buffer_reader = io::BufReader::new(input_file_ptr);

                // seek to later part of single input file
                if !_args.skip_merge {
                    buffer_reader
                        .seek(SeekFrom::Start(i * single_thread_read_size))
                        .expect("Seek failed");
                }

                let end_of_read = match _args.skip_merge {
                    true => input_file_metadata.len(),
                    false => match i == 40 {
                        true => file_size - ((num_cores_avail - 1) * single_thread_read_size),
                        false => single_thread_read_size,
                    },
                };

                move || {
                    let mut data_count = 0;
                    for line in buffer_reader.lines() {
                        let line_unwrapped = line.unwrap().clone();
                        let line_size = line_unwrapped.chars().count() as u64;
                        data_count += line_size;
                        if data_count >= end_of_read {
                            //exit read loop
                            break;
                        }

                        let object: Value = match serde_json::from_str(line_unwrapped.as_str()) {
                            Ok(v) => v,
                            Err(_) => {
                                continue;
                            }
                        };

                        let paper_year = match object["publication_year"].as_i64() {
                            Some(v) => v.to_string(),
                            None => {
                                println!("[W] {}", "Cannot get publication year");
                                continue;
                            }
                        };

                        let pap_id = match object["id"].as_str() {
                            Some(v) => v.to_string(),
                            None => {
                                println!("[W] {}", "Cannot get paper ID");
                                continue;
                            }
                        };

                        // check if papers are affiliated to a given country in that year
                        let mut paper_is_affiliated: bool = false;

                        for _authors in object["authorships"].as_array().unwrap() {
                            let author_id = match _authors["author"]["id"].as_str() {
                                Some(id) => id.to_string(),
                                None => {
                                    println!(
                                        "[W] {} {} {}",
                                        "Found paper (".yellow(),
                                        pap_id,
                                        ") with no author_id".yellow()
                                    );
                                    continue;
                                }
                            };

                            if let Some(author_entry) = _author_filter_map.get(&author_id) {
                                if let Some(aff_ve) = author_entry.get(&paper_year) {
                                    paper_is_affiliated =
                                        paper_is_affiliated || aff_ve.contains(&affiliation);
                                }
                            }

                            if paper_is_affiliated {
                                // if paper is affiliated break from author check loop
                                break;
                            }
                        }

                        if !paper_is_affiliated {
                            progress_bar.lock().unwrap().inc(line_size);
                            // If paper is not affiliated, go to next paper
                            continue;
                        }

                        //filter by topic
                        let concepts = match object["concepts"].as_array() {
                            Some(v) => v,
                            None => {
                                progress_bar.lock().unwrap().inc(line_size);
                                continue;
                            }
                        };

                        if !extract_topics(concepts).contains(&thread_topic_filter_keyword) {
                            progress_bar.lock().unwrap().inc(line_size);
                            continue;
                        }

                        writeln!(_output_file, "{}", line_unwrapped)
                            .expect("Unable to write entry");
                        progress_bar.lock().unwrap().inc(line_size);
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle
                .join()
                .unwrap_or_else(|_| println!("{}", "Unable joining thread".yellow()));
        }

        println!("[i] {}", "Completed filter phase".green());
        println!("[i] {}", "Starting filter-merge phase".yellow());

        let mut files_to_merge: Vec<String> = Vec::new();

        for i in 0..num_cores_avail {
            files_to_merge.push("dataset.part.".to_string() + i.to_string().as_str());
        }

        merge_files(&files_to_merge, "papers-filtered.jsonl")
            .expect("Unable to merge dataset files");
        println!("[i] {}", "Completed filter-merge phase".green());
    }
}
