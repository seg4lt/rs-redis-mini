[![progress-banner](https://backend.codecrafters.io/progress/redis/a45bcf86-4269-43cb-8630-c494ce35d0f1)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# CodeCrafters Redis Challenge

Rust implementation of the Redis based on code crafters challenge. The goal of the project was to learn more about Rust and get better at the language.

Also, instead of using `Arc<Mutex<T>>` everywhere, I decided to use `channels` to communicate between threads.

## Next Steps

- [ ] Refactor the code so it is more redable.
  - At one point, I started to rush and focus only on making test pass 😅.

## Tips

Create a rdb file for testing

```sh
Terminal #1> redis-server

Terminal #2> redis-cli set mykey myval
Terminal #2> redis-cli save

Terminal #1:
Press ctrl + c to shut down redis-server. You'll see dump.rdb created
To inspect its contents you can use: hexdump -C dump.rdb
```
