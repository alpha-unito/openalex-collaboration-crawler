pub mod graph_utils {
    use colored::Colorize;
    use flate2::read::GzDecoder;
    use indicatif::{ProgressBar, ProgressStyle};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::env;
    use std::ffi::OsStr;
    use std::fs::{File, OpenOptions};
    use std::io::{self, BufRead, Read, Write};
    use std::process::Command;
    use std::{fs, path, thread};
    use walkdir::WalkDir;

    pub fn get_num_threads() -> u64 {
        let mut num_cores_avail: u64 = match thread::available_parallelism() {
            Ok(n) => n.get() as u64,
            Err(_) => 1,
        };

        // Use GRAPH_NUM_THREADS to override max number of threads
        num_cores_avail = match env::var("GRAPH_NUM_THREADS") {
            Ok(n) => n.parse::<u64>().unwrap_or(num_cores_avail),
            Err(_) => num_cores_avail,
        };

        println!(
            "[i] {} {} {}",
            "Utilizing".blue(),
            num_cores_avail,
            "threads".blue()
        );

        num_cores_avail
    }

    pub fn process_directories(input_dir: String) -> Vec<String> {
        let mut paths: Vec<String> = Vec::new();

        for entry in WalkDir::new(input_dir) {
            let entry = entry.unwrap();
            if entry.file_type().is_file() && entry.path().extension() == Some(OsStr::new("gz")) {
                paths.push(entry.path().display().to_string());
            }
        }
        paths
    }

    pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where
        P: AsRef<path::Path>,
    {
        let file = File::open(filename)?;
        Ok(io::BufReader::new(file).lines())
    }

    pub fn merge_files(source_files: &Vec<String>, output_file: &str) -> io::Result<()> {
        // Open the output file for writing (create or append mode)
        let mut output = OpenOptions::new()
            .create(true)
            .append(true) // Append to the file instead of overwriting it
            .open(output_file)?;

        let bar = ProgressBar::new(source_files.len() as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "[i] Merging files: {bar:40.cyan/blue} {pos:>7}/{len:7} [{elapsed_precise}]",
            )
            .unwrap()
            .progress_chars("##-"),
        );

        // Iterate over each source file
        for file in source_files {
            bar.inc(1);

            // Open the current source file
            let mut input = File::open(file)?;

            // Read the content of the source file
            let mut content = String::new();
            input.read_to_string(&mut content)?;

            // Write the content to the output file
            output.write_all(content.as_bytes())?;
        }

        let cwd = std::env::current_dir()?.display().to_string() + "/";
        for file in source_files {
            fs::remove_file(cwd.clone() + file.as_str()).expect("Unable to remove file");
        }

        Ok(())
    }

    pub fn decompress_gz_to_memory(file_path: String) -> io::Result<Vec<String>> {
        // Open the .gz file
        let file = File::open(file_path)?;

        // Create a GzDecoder to handle decompression
        let mut decoder = GzDecoder::new(file);

        // Read decompressed content into a Vec<u8>
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;

        Ok(match String::from_utf8(decompressed_data) {
            /*
               1) Split text into different &str by the char \n, obtaining an iterator over it

               2) Apply a map, with the operator (that is applied to each element of the iterator)
                  String::from(&str), to convert all the str to Strings

               3) Convert the result of the map, from an iterable object to a Vec<String>
            */
            Ok(parsed) => parsed.split('\n').map(String::from).collect(),
            Err(error) => vec![error.to_string()],
        })
    }

    pub fn get_line_count(input_filename: &String) -> u64 {
        let get_lines_command = Command::new("wc")
            .arg("-l")
            .arg(input_filename)
            .output()
            .expect("Failed to compute line count for file");

        let input_line_count_str: String = String::from_utf8(get_lines_command.stdout).unwrap();

        let mut input_line_count: u64 = 0;

        for word in input_line_count_str.split_whitespace() {
            if let Ok(number) = word.parse::<u64>() {
                input_line_count = number;
            }
        }

        input_line_count
    }

    pub fn load_authors_from_file(
        filename: &String,
    ) -> HashMap<String, HashMap<String, Vec<String>>> {
        let mut authors: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

        for line in read_lines(filename).unwrap() {
            let obj: Value = serde_json::from_str(&line.unwrap()).unwrap();
            let id = obj["id"].as_str().unwrap();
            let author_affiliation = authors.entry(id.to_string()).or_insert(HashMap::new());

            for year in obj["affs"].as_object().unwrap() {
                let mut _year_map = author_affiliation
                    .entry(year.0.clone())
                    .or_insert(Vec::new());
                let _years = year.1.as_array().unwrap();
                for country in _years {
                    _year_map.push(country.as_str().unwrap().to_string());
                }
            }
        }

        println!(
            "[i] {} {} {}",
            "Loaded".blue(),
            authors.len(),
            "authors to memory ".blue()
        );

        authors
    }

    pub fn parse_json_author_line(json_inut: &String) -> (String, Vec<(String, String)>) {
        let object: Value = serde_json::from_str(json_inut).unwrap();

        let openalex_id = object["id"]
            .as_str()
            .unwrap_or_else(|| "not found")
            .to_owned();

        let mut latest_aff_vec = Vec::new();

        match object["affiliations"].as_array() {
            Some(affiliations) => {
                for affiliation in affiliations {
                    let institution = affiliation["institution"]["country_code"]
                        .as_str()
                        .unwrap_or_else(|| "No institution found");
                    let years = affiliation["years"].as_array().unwrap();
                    for year in years {
                        latest_aff_vec.push((
                            institution.to_owned(),
                            year.as_i64().unwrap_or_else(|| -1).to_string().to_owned(),
                        ));
                    }
                }
            }
            None => {}
        };

        (openalex_id, latest_aff_vec)
    }

    pub fn process_single_author_file(file_path: String, output_file: &mut File) {
        match decompress_gz_to_memory(file_path) {
            Ok(data) => {
                for value in data {
                    if value.is_empty() {
                        continue;
                    }
                    let (id, aff) = parse_json_author_line(&value);
                    if aff.is_empty() {
                        continue;
                    }

                    match write!(output_file, "{{ \"id\":\"{}\",\"affs\":[", id) {
                        Ok(_status) => {}
                        Err(error) => println!("Failed to write to file: {}", error),
                    }
                    for (pos, year) in aff.iter().enumerate() {
                        match write!(output_file, "{{\"{}\":\"{}\"}}", year.1, year.0) {
                            Ok(_status) => {}
                            Err(error) => println!("Failed to write to file: {}", error),
                        }
                        if pos != aff.len() - 1 {
                            match write!(output_file, ",") {
                                Ok(_status) => {}
                                Err(error) => println!("Failed to write to file: {}", error),
                            }
                        }
                    }
                    match writeln!(output_file, "]}}") {
                        Ok(_status) => {}
                        Err(error) => println!("Failed to write to file: {}", error),
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to decompress file: {}", e);
            }
        }
    }

    //TODO: check also that author is present in list and year of paper match affiliation
    pub fn process_single_work_file(
        file_path: String,
        output_file: &mut File,
        authors: &Vec<String>,
    ) {
        let papers = decompress_gz_to_memory(file_path).unwrap();
        for paper in papers {
            if paper.is_empty() {
                continue;
            }

            let mut paper_authors: Vec<String> = Vec::new();

            let paper_obj: Value = serde_json::from_str(&paper).unwrap();
            match paper_obj["authorships"].as_array() {
                Some(auth_list) => {
                    for authors in auth_list {
                        match authors["author"]["id"].as_str() {
                            Some(id) => paper_authors.push(id.to_string()),
                            None => continue,
                        };
                    }
                }
                None => continue,
            };

            if paper_authors
                .iter()
                .any(|person| authors.binary_search(&person).is_ok())
            {
                writeln!(output_file, "{}", paper).unwrap();
            }
        }
    }

    pub fn extract_topics(concepts: &Vec<Value>) -> Vec<String> {
        let mut topics = Vec::new();
        for concept in concepts {
            let topic_name = match concept["display_name"].as_str() {
                Some(name) => name.to_string(),
                None => {
                    continue;
                }
            };
            topics.push(topic_name);
        }

        topics
    }
}
