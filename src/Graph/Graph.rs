use clap::Parser;
use colored::Colorize;
use openalex_collaboration_crawler::graph_utils::{extract_topics, get_num_threads, merge_files, read_lines};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, Seek, SeekFrom};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{io, thread};
use unordered_pair;
use unordered_pair::UnorderedPair;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(
        long,
        help = "Format on how to split years. Colon separated list of year entries. Each entry \
                is specified by <starting_year>-<ending_year>. If either <starting_year> or \
                <ending_year> are missing, then the minimum or maximum year found is taken \
                into consideration."
    )]
    format: Option<String>,

    #[arg(
        long,
        help = "List of papers from which to extract a graph in JSONL format"
    )]
    input: Option<String>,

    #[arg(long, help = "Output file base name. optionsl")]
    output: Option<String>,

    #[arg(
        long,
        help = "Generate weighted graph of the file given as parameter. If this option is used, \
                then no other action will occur once the weighted graph has been generated"
    )]
    extract_weighted: Option<String>,
}

#[derive(Debug, Clone)]
struct TimeInterval {
    start: u64,
    end: u64,
    file_name: String,
}

fn is_u64(s: &str) -> bool {
    s.parse::<u64>().is_ok()
}

fn main() {
    let _args = Args::parse();

    let input_file_name = _args
        .input
        .unwrap_or_else(|| "papers-filtered.jsonl".to_string());

    if let Some(weight_input_file) = _args.extract_weighted {
        println!(
            "[i] {} {}",
            "Will generate weighted graph from CSV file".blue(),
            weight_input_file.yellow()
        );

        let in_file = match File::open(&weight_input_file) {
            Ok(file) => file,
            Err(e) => {
                println!("[F] {} {}", "Error: ".red(), e);
                exit(-1);
            }
        };

        let bar = ProgressBar::new(in_file.metadata().unwrap().len() as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "[i] Loading & compressing CSV: [{wide_bar:.cyan/blue}] {percent}%",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        let mut weights: HashMap<UnorderedPair<String>, u64> = HashMap::new();

        for line in read_lines(&weight_input_file).unwrap() {
            let line_unwrapped = line.unwrap();
            bar.inc((line_unwrapped.len() + 1) as u64);

            let line_items: Vec<String> =
                line_unwrapped.split(',').map(|x| x.to_string()).collect();

            let key = UnorderedPair(line_items[2].clone(), line_items[3].clone());

            *weights.entry(key).or_insert(0) += 1;
        }

        println!(
            "[i] {} {} {}",
            "Loaded ".green(),
            weights.len(),
            "edges.".green()
        );

        let outfilename = "weighted_".to_string() + weight_input_file.split("/").last().unwrap();

        println!("[i] {} {}", "Storing data to:".blue(), outfilename);
        let mut output_file = File::create(outfilename).unwrap();
        for (k, v) in weights {
            writeln!(output_file, "{},{},{}", k.0, k.1, v).expect("Error writing to file.");
        }

        println!("[i] {}", "Done".green(),);

        exit(0);
    }

    let output_file_name = _args.output.unwrap_or_else(|| "dataset.csv".to_string());
    let metadata_file_name = "metadata_".to_string() + output_file_name.clone().as_str();

    let mut time_intervals: Vec<TimeInterval> = Vec::new();
    let mut output_file_pointers: Vec<Arc<Mutex<File>>> = Vec::new();

    if let Some(format) = _args.format {
        let formats: Vec<String> = format.split(',').map(str::to_string).collect();
        for interval in formats {
            let mut values: Vec<String> = Vec::new();
            for (i, part) in interval.split('-').enumerate() {
                if i > 0 {
                    values.push("-".to_string());
                }
                if !part.is_empty() {
                    values.push(part.to_string());
                }
            }

            let start = match is_u64(&values.first().unwrap()) {
                true => values.first().unwrap().parse::<u64>().unwrap(),
                false => 0,
            };

            let end = match is_u64(&values.last().unwrap()) {
                true => values.last().unwrap().parse::<u64>().unwrap(),
                false => u64::MAX,
            };

            let file_name =
                interval.replace("-", "_").to_string() + "_" + output_file_name.clone().as_str();

            time_intervals.push(TimeInterval {
                start,
                end,
                file_name: file_name.clone(),
            });

            output_file_pointers.push(match File::create(file_name) {
                Ok(file) => Arc::new(Mutex::new(file)),
                Err(_) => {
                    println!("[E] {}", "Unable to open output file".red());
                    exit(-1);
                }
            });
        }
    } else {
        time_intervals.push(TimeInterval {
            start: 0,
            end: u64::MAX,
            file_name: "all_dataset.csv".to_string(),
        });
        output_file_pointers.push(match File::create("all_dataset.csv") {
            Ok(file) => Arc::new(Mutex::new(file)),
            Err(_) => {
                println!("[E] {}", "Unable to open output file".red());
                exit(-1);
            }
        });
    }

    println!(
        "[i] {} {}",
        "Filtering papers from".blue(),
        input_file_name.yellow()
    );

    println!(
        "[i] {}",
        "Will generate the following temporal adjacency lists:".blue()
    );
    for (offset, interval) in time_intervals.iter().enumerate() {
        let start_year = match interval.start {
            0 => "START".to_string(),
            _ => interval.start.to_string(),
        };

        let end_year = match interval.end {
            u64::MAX => "END".to_string(),
            _ => interval.end.to_string(),
        };

        println!(
            "\t {} {} -> from {} to {} -> [{}]",
            "interval".blue(),
            offset,
            start_year,
            end_year,
            interval.file_name.yellow()
        );
    }

    let file_size = match File::open(&input_file_name) {
        Ok(file) => file.metadata().unwrap().len(),
        Err(_) => {
            println!(
                "[E] {}",
                "Unable to open input file / unable to get input file size".red()
            );
            exit(-1);
        }
    };

    let _bar = ProgressBar::new(file_size);
    _bar.set_style(
        ProgressStyle::with_template(
            "[i] Processed: [{wide_bar:.cyan/blue}] {decimal_bytes}/{decimal_total_bytes}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    let bar = Arc::new(Mutex::new(_bar));

    let num_cores_avail = get_num_threads();

    let single_thread_read_size = file_size / num_cores_avail;

    let mut handles = vec![];

    for i in 0..num_cores_avail {
        let starting_offset = i * single_thread_read_size;
        let handle = thread::spawn({
            let progress_bar = Arc::clone(&bar);

            let mut _metadata_file =
                File::create("metadata.part.".to_string() + i.to_string().as_str()).unwrap();

            let mut buffer_reader = io::BufReader::new(File::open(&input_file_name).unwrap());
            buffer_reader
                .seek(SeekFrom::Start(starting_offset))
                .expect("Seek failed");

            let end_of_read = match i == 40 {
                true => file_size - (39 * single_thread_read_size),
                false => single_thread_read_size,
            };

            let output_file = Arc::new(output_file_pointers.clone());
            let time_intervals = time_intervals.clone();

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

                    progress_bar.lock().unwrap().inc(line_size);

                    let object: Value = match serde_json::from_str(line_unwrapped.as_str()) {
                        Ok(v) => v,
                        Err(_) => {
                            continue;
                        }
                    };

                    let id = match object["id"].as_str() {
                        Some(v) => v.replace("https://openalex.org/", ""),
                        None => {
                            continue;
                        }
                    };

                    //filter by topic
                    let concepts = match object["concepts"].as_array() {
                        Some(v) => v,
                        None => {
                            continue;
                        }
                    };

                    let topics = extract_topics(concepts);

                    let paper_year = match object["publication_year"].as_u64() {
                        Some(year) => year,
                        None => {
                            continue;
                        }
                    };

                    let mut author_vector = Vec::new();

                    match object["authorships"].as_array() {
                        Some(authors) => {
                            for _authors in authors {
                                match _authors["author"]["id"].as_str() {
                                    Some(id) => author_vector
                                        .push(id.to_string().replace("https://openalex.org/", "")),
                                    None => {
                                        continue;
                                    }
                                };
                            }
                        }
                        None => {
                            continue;
                        }
                    };

                    let topic_list = topics.join(";");

                    let mut edges: Vec<String> = Vec::new();

                    if author_vector.len() == 1 {
                        // Handle the special case of a single string
                        edges.push(format!(
                            "{},{},{},{}",
                            paper_year, id, author_vector[0], author_vector[0]
                        ));
                    } else {
                        // Generate all unique combinations of two strings
                        for i in 0..author_vector.len() {
                            for j in (i + 1)..author_vector.len() {
                                edges.push(format!(
                                    "{},{},{},{}",
                                    paper_year, id, author_vector[i], author_vector[j]
                                ));
                            }
                        }
                    }

                    // add metadata entry
                    writeln!(_metadata_file, "{},{}", id, topic_list)
                        .expect("Unable to write entry");

                    // Write to correct time frame adjacency list file
                    for (index, time_frame) in time_intervals.iter().enumerate() {
                        if paper_year >= time_frame.start && paper_year <= time_frame.end {
                            let mut file = output_file[index].lock().unwrap();
                            // for edge in edges {
                            writeln!(file, "{}", edges.join("\n"))
                                .expect("[F] unable to write to output file");
                            //}

                            // once write occurs, break from current loop
                            break;
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("[i] {}", "Completed adjacency list generation".green());

    let mut metadata_to_merge: Vec<String> = Vec::new();

    for i in 0..num_cores_avail {
        metadata_to_merge.push("metadata.part.".to_string() + i.to_string().as_str());
    }

    println!(
        "[i] {} {}",
        "Storing adjacency list metadata to".blue(),
        metadata_file_name.yellow()
    );
    merge_files(&metadata_to_merge, &metadata_file_name).expect("Unable to merge metadata files");

    println!("[i] {}", "Done merging metadata files...".green());
}
