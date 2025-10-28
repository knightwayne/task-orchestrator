# Task Orchestrator

Asynchronous Task Orchestrator written in Rust to concurrently process HTTP request. This program uses a task pool (using tokio runtime) and asynchronously executes the blueprint task of fetch-sleep-print. serde and csv crates are used to read/write to CSV files.

[![Rust CI](https://github.com/knightwayne/task-orchestrator/actions/workflows/main.yml/badge.svg)](https://github.com/knightwayne/task-orchestrator/actions/workflows/main.yml)

## Design Approach

1. Program starts in ```async main()```, it parses command line input to get the input CSV file path.

2. It then reads the CSV and deserializes it using ```serde```, and csv row data into task_vector as struct (task_id, task_type).

3. Task pool: For each task, a new asynchronous job is spawned using ```tokio::spawn```,  and ```execute_task()``` function is called concurrently.

4. ```execute_task()``` follows the defined blueprint - 
    1. fetch_data from predefined ```URL``` using ```reqwest```.
    2. sleep (using tokio) for 5s. 
    3. if task is successful, print message to stderr.

    If fetch is unsuccessful, final status is ```failed```, if ```task_type != process_data ```, final status is ```skipped```.

5. Completed tasks are awaited and stored in a ```result_vector```, and again using ```serde``` and ```csv```, structs are serialized and written to the output CSV (stdout) specified in command line.

## Setup

1. Install Rust (stable):

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable
```

2. Clone the repository:

```
git clone <repo-url>
cd <repo-folder>
```

3. Add dependencies:

```
cargo add tokio reqwest csv serde serde_derive
```

## Build and Run

```
cargo build
cargo run -- <input_csv_path> > results.csv
```

Example:

```
cargo run -- ./src/io_files/input.csv > ./src/io_files/output.csv
```


## Program Assumptions

* The assessment requirements can be done in both sequential manner, running tasks one by one, or running tasks concurrently using asynchronous behavior. 
* Input file is assumed to have 2 columns only --  `task_id` (u64), `task_type` (string). `task_id` is unique, and `task_type` is `process_data`. If the later isn't true, the final_status for the respective task is marked `Failed`. 
* If the task_type is not `process_data`, `unsupported task_type:` is logged in `error_info` column. 
* The URL to fetch request from is fixed (hardcoded in the program) in `URL` variable. 
* If the program is unable to fetch from the URL, `fetch_data failed` is logged in `error_info` column. 
* The current collection method awaits the task handle in their order of creation (i.e. same as the input csv file), hence the output csv file tends to follow the same order. An alternative way to implement this would be using an unordered pool collector (e.g., `FuturesUnordered`) which would record task results in output csv in actual order of completion rather than order of awaiting their completion status. The actual order of task completion is completely asynchronous, and tasks can be completed in any order, as seen in the stderr. 