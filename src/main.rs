use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Instant,
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "prun")]
struct Opt {
    #[structopt(name = "file", parse(from_os_str), help = "Specifies the config file")]
    config: PathBuf,

    #[structopt(short, long, help = "Prints debug information while running")]
    verbose: bool,

    #[structopt(
        short,
        long,
        help = "Specifies the number of process that a running concurrently"
    )]
    num_threads: Option<usize>,

    #[structopt(short, long, parse(from_os_str), help = "Specifies the output file")]
    output: Option<PathBuf>,
}

type Tasks = HashMap<String, Task>;

#[derive(Serialize, Deserialize, Debug)]
struct Task {
    command: String,
    args: Vec<Argument>,
}

impl Task {
    fn to_concreate_tasks(&self, name: &str) -> Vec<Cmd> {
        let mut res = Vec::new();

        fn p(
            args: &[Argument],
            mut so_far: Vec<String>,
            so_far_name: String,
            res: &mut Vec<(Vec<String>, String)>,
        ) {
            if args.is_empty() {
                res.push((so_far, so_far_name));
                return;
            }

            match &args[0] {
                Argument::Static(str) => {
                    so_far.push(str.clone());
                    p(&args[1..], so_far, so_far_name, res)
                }
                Argument::Choice(opts) => {
                    for opt in opts {
                        let mut sf = so_far.clone();
                        sf.push(opt.clone());

                        let mut sfn = so_far_name.clone();
                        sfn.push(' ');
                        sfn.push_str(opt);

                        p(&args[1..], sf, sfn, res)
                    }
                }
                Argument::Range(range) => match range {
                    RangeObject::Int {
                        from,
                        to,
                        step,
                        prefix,
                    } => {
                        let mut c = *from;
                        while c <= *to {
                            let mut sf = so_far.clone();
                            sf.push(format!(
                                "{}{}",
                                prefix.as_ref().unwrap_or(&String::new()),
                                c
                            ));

                            let mut sfn = so_far_name.clone();
                            sfn.push(' ');
                            sfn.push_str(&format!("{}", c));

                            p(&args[1..], sf, sfn, res);
                            c += *step;
                        }
                    }
                    RangeObject::Float {
                        from,
                        to,
                        step,
                        prefix,
                    } => {
                        let mut c = *from;
                        while c <= *to {
                            let mut sf = so_far.clone();
                            sf.push(format!(
                                "{}{}",
                                prefix.as_ref().unwrap_or(&String::new()),
                                c
                            ));

                            let mut sfn = so_far_name.clone();
                            sfn.push(' ');
                            sfn.push_str(&format!("{}", c));

                            p(&args[1..], sf, sfn, res);
                            c += *step;
                        }
                    }
                },
            }
        }

        p(&self.args, Vec::new(), name.to_string(), &mut res);

        res.into_iter()
            .map(|(args, name)| {
                let mut cmd = Command::new(&self.command);
                cmd.stdout(Stdio::piped());
                for arg in args {
                    cmd.arg(arg);
                }
                Cmd { command: cmd, name }
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "content")]
enum Argument {
    Static(String),
    Range(RangeObject),
    Choice(Vec<String>),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum RangeObject {
    Int {
        from: i32,
        to: i32,
        step: i32,
        prefix: Option<String>,
    },
    Float {
        from: f64,
        to: f64,
        step: f64,
        prefix: Option<String>,
    },
}

struct Cmd {
    command: Command,
    name: String,
}

fn main() {
    let opt = Opt::from_args();
    if !opt.config.exists() {
        eprintln!("Could not find config file '{:?}'", opt.config);
        return;
    }

    let mut file = match File::open(&opt.config) {
        Ok(f) => BufReader::new(f),
        Err(e) => {
            eprintln!("Could not open config file '{:?}': {}", opt.config, e);
            return;
        }
    };

    let mut string = String::new();
    if let Err(e) = file.read_to_string(&mut string) {
        eprintln!("Failed to read config file '{:?}': {}", opt.config, e);
        return;
    }

    let tasks = match toml::from_str::<Tasks>(&string) {
        Ok(tasks) => tasks,
        Err(e) => {
            eprintln!("Failed to parse config file'{:?}': {}", opt.config, e);
            return;
        }
    };

    let tasks = tasks
        .into_iter()
        .map(|(name, cmd)| {
            cmd.to_concreate_tasks(&name).into_iter()
            // .map(|cmd| (name.clone(), cmd))
        })
        .flatten()
        .collect::<VecDeque<_>>();

    let n = opt
        .num_threads
        .unwrap_or(num_cpus::get() / 2)
        .min(tasks.len());

    let output = if let Some(output) = opt.output.clone() {
        let f = match OpenOptions::new()
            .append(true)
            .write(true)
            .create(true)
            .open(&output)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open output file '{:?}': {}", output, e);
                return;
            }
        };

        Some(BufWriter::new(f))
    } else {
        None
    };

    let output = Arc::new(Mutex::new(output));

    println!("[PRUN] Running {} tasks on {} processes", tasks.len(), n);

    let mut handles = Vec::with_capacity(n);
    let tasks = Arc::new(Mutex::new(tasks));

    let verbose = opt.verbose;

    for i in 0..n {
        let tasks = tasks.clone();
        let output = output.clone();
        let handle = std::thread::spawn(move || {
            if verbose {
                println!("[Worker #{}] Initalized", i);
            }

            loop {
                let mut lock = tasks.lock().unwrap();
                let task = lock.pop_front();
                drop(lock);

                if let Some(task) = task {
                    let Cmd { mut command, name } = task;
                    if verbose {
                        println!("[Worker #{}] Running task: {:?}", i, name);
                    }
                    let mut child = command.spawn().unwrap();
                    let t0 = Instant::now();
                    child.wait().unwrap();
                    let t1 = Instant::now();

                    if verbose {
                        println!(
                            "[Worker #{}] Completed task: {:?} in {:?}",
                            i,
                            name,
                            t1 - t0
                        );
                    }
                    let output = output.lock();
                    if let Ok(mut output) = output {
                        if let Some(output) = output.as_mut() {
                            writeln!(output, "{}: {:?}", name, t1 - t0).unwrap();
                            output.flush().unwrap();
                        }
                    }
                } else {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()
    }
}
