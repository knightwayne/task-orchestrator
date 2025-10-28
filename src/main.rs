use std::fs::File;
use std::env;
// use std::io::stdout;
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio::time::{sleep, Duration};
use tokio::task;
// use futures::future::join_all;
use reqwest;

static URL: &str = "https://httpbin.org/get";

#[derive(Debug, Deserialize)]
struct TaskInput {
    task_id: u64,
    task_type: String
}

#[derive(Debug, Serialize)]
struct TaskOutput {
    task_id: u64,
    final_status: String,
    error_info: String
}

// Async function
async fn execute_task(task: TaskInput) -> TaskOutput {

    let mut output = TaskOutput {
        task_id: task.task_id,
        final_status: "".to_string(),
        error_info: "".to_string(),
    };

    if task.task_type != "process_data" {
        output.final_status = "Failed".to_string();
        output.error_info = format!("unsupported task_type: {}", task.task_type);
        return output;
    }

    // fetch_data
    match reqwest::get(URL).await {
        Ok(_) => {},
        Err(e) => {
            output.final_status = "Failed".to_string();
            output.error_info = format!("fetch_data failed: {}", e);
            return output;
        }
    }

    // sleep to pause
    sleep(Duration::from_millis(5000)).await;

    // output 
    eprintln!("Task {} completed successfully", task.task_id);
    output.final_status = "Completed".to_string();
    output.error_info = "".to_string();

    return output;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -- <input_csv_path> > output.csv");
        std::process::exit(1);
    }

    let input_path = &args[1];

    //Input
    let file = File::open(input_path)?;
    let mut rdr = csv::Reader::from_reader(file);

    let mut task_vector: Vec<TaskInput> = Vec::new();
    for result in rdr.deserialize() {
        let record: TaskInput = result?;
        task_vector.push(record);
    }

    // Task Pool & Initialize
    let mut handles = Vec::new();
    for task in task_vector {
        let handle = task::spawn(execute_task(task));
        handles.push(handle);
    }

    // JoinHandle
    // futures::future::join_all also preserves the order of the input futures.
    let mut result_vector: Vec<TaskOutput> = Vec::new();
    for handle in handles {
        if let Ok(task_output) = handle.await {
            result_vector.push(task_output);
        }
    }

    //Output
    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    for r in result_vector {
        wtr.serialize(r)?;
    }
    wtr.flush()?;

    return Ok(());
}