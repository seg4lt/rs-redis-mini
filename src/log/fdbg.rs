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

#[macro_export]
macro_rules! binary {
    (@msb; $num:expr, $n:expr) => {{
        let shift = 8 - $n;
        let result: u8 = $num >> shift;
        result
    }};
    (@lsb; $num:expr, $n:expr) => {{
        let shift = 8 - $n;
        let left_shift: u8 = $num << shift;
        let right_shift: u8 = left_shift >> shift;
        right_shift
    }};
}

// fn get_lsb(num: u8, n: usize) -> u8 {
//     let shift = 8 - n;
//     (num << shift) >> shift
// }

// fn get_msb(num: u8, n: usize) -> u8 {
//     num >> n
// }
