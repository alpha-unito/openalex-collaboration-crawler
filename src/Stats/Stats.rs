use indicatif::ProgressBar;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, Seek, SeekFrom, Write};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{env, fs, io, thread};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Usage: ./dataset-stats <paper_source_file>");
        exit(-1);
    }

    /*
        This map stores the count of how many papers have been published in a given year
    */
    let papers_per_year: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    /*
     A map, indexed by year for which every year if an author has produced something, it is then
     inserted into itself
    */
    let authors_per_year: Arc<Mutex<HashMap<u64, HashSet<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    /*
        This map contains the count for how many papers any given author has published
    */
    let papers_per_author: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    /*
       This map counts how many papers have N (i.e. the key of the mao) authors
    */
    let paper_author_count: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    /*
       This map stores the amount of papers per a given topic
    */
    let topic_count: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    let input_file = File::open(&args[1]).unwrap();
    let file_size = input_file.metadata().unwrap().len();

    let skipped_papers: u64 = 0;

    let bar = Arc::new(Mutex::new(ProgressBar::new(file_size)));

    let num_cores_avail: u64 = match thread::available_parallelism() {
        Ok(n) => n.get() as u64,
        Err(_) => 1,
    };

    let single_thread_read_size = file_size / num_cores_avail;

    let mut handles = vec![];

    for i in 0..num_cores_avail {
        let starting_offset = i * single_thread_read_size;
        let handle = thread::spawn({
            let mut buffer_reader = io::BufReader::new(File::open(&args[1]).unwrap());
            buffer_reader
                .seek(SeekFrom::Start(starting_offset))
                .expect("Seek failed");

            let end_of_read = match i == 40 {
                true => file_size - (39 * single_thread_read_size),
                false => single_thread_read_size,
            };
            let progress_bar = Arc::clone(&bar);
            let skipped_papers_mut = Arc::new(Mutex::new(skipped_papers));
            let master_papers_per_year = papers_per_year.clone();
            let master_authors_per_year = authors_per_year.clone();
            let master_paper_author_count = paper_author_count.clone();
            let master_papers_per_author = papers_per_author.clone();
            let master_topic_count = topic_count.clone();

            move || {
                let mut thread_papers_per_year: HashMap<u64, u64> = HashMap::new();
                let mut thread_authors_per_year: HashMap<u64, HashSet<String>> = HashMap::new();
                let mut thread_papers_per_author: HashMap<String, u64> = HashMap::new();
                let mut thread_paper_author_count: HashMap<u64, u64> = HashMap::new();
                let mut thread_topic_count: HashMap<String, u64> = HashMap::new();

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
                            *skipped_papers_mut.lock().unwrap() += 1;
                            continue;
                        }
                    };

                    let paper_year = match object["publication_year"].as_u64() {
                        Some(year) => year,
                        None => {
                            *skipped_papers_mut.lock().unwrap() += 1;
                            continue;
                        }
                    };

                    match thread_papers_per_year.get(&paper_year) {
                        Some(count) => {
                            thread_papers_per_year.insert(paper_year, count + 1);
                        }
                        None => {
                            thread_papers_per_year.insert(paper_year, 1);
                        }
                    }

                    let authors_vector = match object["authorships"].as_array() {
                        Some(v) => v,
                        None => {
                            *skipped_papers_mut.lock().unwrap() += 1;
                            continue;
                        }
                    };

                    //add the size of authors to the distrib
                    let entry = thread_paper_author_count
                        .entry(authors_vector.len() as u64)
                        .or_insert(0);
                    *entry += 1;

                    //add the author to the list of author active in a certain year
                    for _authors in authors_vector {
                        let author_id = match _authors["author"]["id"].as_str() {
                            Some(id) => id.to_string(),
                            None => {
                                *skipped_papers_mut.lock().unwrap() += 1;
                                continue;
                            }
                        };

                        let author_count_entry = thread_authors_per_year
                            .entry(paper_year)
                            .or_insert(HashSet::new());

                        if !author_count_entry.contains(&author_id) {
                            let id = author_id.clone();
                            let _ = author_count_entry.insert(id);
                        }

                        //Update the amount of papers for a single author
                        let author_entry = thread_papers_per_author.entry(author_id).or_insert(0);
                        *author_entry += 1;
                    }

                    let concepts = match object["concepts"].as_array() {
                        Some(v) => v,
                        None => {
                            *skipped_papers_mut.lock().unwrap() += 1;
                            continue;
                        }
                    };

                    for concept in concepts {
                        let topic_name = match concept["display_name"].as_str() {
                            Some(name) => name.to_string(),
                            None => {
                                *skipped_papers_mut.lock().unwrap() += 1;
                                continue;
                            }
                        };

                        thread_topic_count
                            .entry(topic_name)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }

                    progress_bar.lock().unwrap().inc(line_size);
                }

                println!(
                    "Thread {} has completed reading data... merging into main structures",
                    i
                );
                //merge data

                for (k, v) in thread_papers_per_year {
                    master_papers_per_year
                        .lock()
                        .unwrap()
                        .entry(k)
                        .and_modify(|i| *i += v)
                        .or_insert(v);
                }

                for (k, v) in thread_papers_per_author {
                    master_papers_per_author
                        .lock()
                        .unwrap()
                        .entry(k)
                        .and_modify(|i| *i += v)
                        .or_insert(v);
                }

                for (k, v) in thread_paper_author_count {
                    master_paper_author_count
                        .lock()
                        .unwrap()
                        .entry(k)
                        .and_modify(|i| *i += v)
                        .or_insert(v);
                }

                for (k, v) in thread_authors_per_year {
                    master_authors_per_year
                        .lock()
                        .unwrap()
                        .entry(k)
                        .and_modify(|master_array| {
                            let v_c = v.clone();
                            for author in v_c {
                                master_array.insert(author.clone());
                            }
                        })
                        .or_insert(v);
                }

                for (k, v) in thread_topic_count {
                    master_topic_count
                        .lock()
                        .unwrap()
                        .entry(k)
                        .and_modify(|e| *e += v)
                        .or_insert(v);
                }
            }
        });

        handles.push(handle);
    }

    println!("Waiting for threads to terminate...");
    for handle in handles {
        handle.join().unwrap();
    }

    println!("Completed analysis. printing statistics");

    if skipped_papers != 0 {
        println!(
            "\n\n WARNING: {} papers were skipped for malformed / incomplete JSONL entry\n",
            skipped_papers
        );
    }

    fs::create_dir_all("../../extra").expect("Could not create extra directory");

    println!("Storing Papers per year distribution:");
    let mut papers_per_year_output = File::create("../../extra/PaperPerYearDistrib.csv").unwrap();
    writeln!(papers_per_year_output, "year,number-of-papers").unwrap();
    for item in papers_per_year.lock().unwrap().iter() {
        writeln!(papers_per_year_output, "{},{}", item.0, item.1).expect("Unable to write data");
    }

    println!("Storing distribution of active author per year:");
    let mut authors_per_year_output =
        File::create("../../extra/ActiveAuthorPerYearDistribution.csv").unwrap();
    writeln!(authors_per_year_output, "year,active-author-count").unwrap();
    for item in authors_per_year.lock().unwrap().iter() {
        writeln!(authors_per_year_output, "{},{}", item.0, item.1.len())
            .expect("Unable to write data");
    }

    println!("Storing Number of papers per author distribution:");
    let mut paper_per_author_distribution: HashMap<u64, u64> = HashMap::new();
    for item in papers_per_author.lock().unwrap().iter() {
        let count = paper_per_author_distribution.entry(*item.1).or_insert(0);
        *count += 1;
    }
    let mut author_per_paper_distrib_output =
        File::create("../../extra/AuthorsPerPaperDistribution.csv").unwrap();
    writeln!(
        author_per_paper_distrib_output,
        "papers-with-N-authora,count"
    )
    .unwrap();
    for item in paper_per_author_distribution {
        writeln!(author_per_paper_distrib_output, "{},{}", item.0, item.1)
            .expect("Unable to write data");
    }

    println!("Storing Number of authors per paper distribution:");
    let mut papers_per_author_distribution_output =
        File::create("../../extra/PapersPerAuthorDistribution.csv").unwrap();
    writeln!(
        papers_per_author_distribution_output,
        "authors-with-N-papers,count"
    )
    .unwrap();
    for item in paper_author_count.lock().unwrap().iter() {
        writeln!(
            papers_per_author_distribution_output,
            "{},{}",
            item.0, item.1
        )
        .expect("Unable to write data");
    }

    println!("Storing Papers topic distribution:");
    let mut papers_per_topic_distribution_output =
        File::create("../../extra/PapersPerTopicDistribution.csv").unwrap();
    writeln!(papers_per_topic_distribution_output, "topic,count").unwrap();
    for item in topic_count.lock().unwrap().iter() {
        writeln!(
            papers_per_topic_distribution_output,
            "{},{}",
            item.0.replace(",", " "),
            item.1
        )
        .expect("Unable to write data");
    }
}
