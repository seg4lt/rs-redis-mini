// #[cfg(feature = "file_lines")]
// cargo run --features=file_lines

// #[cfg(feature = "fdbg")]
#[macro_export]
macro_rules! fdbg {
    ($msg:literal $(,)?) => {
        format!("{} - {}", format!("{}:{}", file!(), line!()), $msg)
    };
    ($fmt:expr, $($arg:tt)*) => (format!("{} {}", format!("{}:{}", file!(), line!()), format!($fmt, $($arg)*)));
}

// #[cfg(not(feature = "fdbg"))]
// macro_rules! fdbg {
//     ($msg:literal $(,)?) => {
//         $msg
//     };
// ($fmt:expr, $($arg:tt)*) => (
//         format!($fmt, $($arg)*);
//     )
// }
