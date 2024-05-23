<p style="text-align:center">
	<img src="https://img.shields.io/badge/Docker-2496ED.svg?style=default&logo=Docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/github/go-mod/go-version/stormsync/collector" alt="">
</p>

<p style="text-align:center">
  <img src="image/stormsync-banner.png" width="1792" alt="project-banner">
</p>

<hr>

## Overview

The Stormsync data transformer is an open-source Go project designed for processing  hail, wind, and tornado data that is coming from the collector microservice. After the data is processed it is published back to a Kafka topic.


## Getting Started

**System Requirements:**

* **Go**: `version 1.22.x`

### Installation

<h4>From <code>source</code></h4>

> 1. Clone the repository:
>
> ```console
> $ git clone https://github.com/stormsync/transformer
> ```
>
> 2. Change to the collector directory:
> ```console
> $ cd collector/cmd/transform
> ```
>
> 3. Build the app:
> ```console
> $ go build -o app
> ```

### Usage

<h4>From <code>source</code></h4>

> Run using the command below:
> ```console
>update with config file info
> ```



### Tests

> Run the test suite using the command below from the root directory of the repo:
> ```console
> $ go test ./...
> ```

---

[//]: # ()
[//]: # (## Repository Structure)

[//]: # ()
[//]: # (```sh)

[//]: # (└── /)

[//]: # (    ├── Dockerfile)

[//]: # (    ├── cmd)

[//]: # (    │   └── collect)

[//]: # (    │       └── main.go)

[//]: # (    ├── collector.go)

[//]: # (    ├── collector_test.go)

[//]: # (    ├── go.mod)

[//]: # (    ├── go.sum)

[//]: # (    ├── golangci.yaml)

[//]: # (    ├── image)

[//]: # (    │   └── stormsync-banner.png)

[//]: # (    ├── redis.go)

[//]: # (    ├── reporttype_string.go)

[//]: # (    └── test_data)

[//]: # (        ├── filtered_hail.csv)

[//]: # (        ├── filtered_torn.csv)

[//]: # (        ├── filtered_wind.csv)

[//]: # (        └── nws_office_id_list.txt)

[//]: # (```)

[//]: # (## Project Roadmap)

[//]: # ()
[//]: # (- [X] `► INSERT-TASK-1`)

[//]: # (- [ ] `► INSERT-TASK-2`)

[//]: # (- [ ] `► ...`)

[//]: # ()
[//]: # (---)

## Contributing

Contributions are welcome! Here are several ways you can contribute:

- **[Report Issues](https://local//issues)**: Submit bugs found or log feature requests for the `` project.
- **[Submit Pull Requests](https://local//blob/main/CONTRIBUTING.md)**: Review open PRs, and submit your own PRs.
- **[Join the Discussions](https://local//discussions)**: Share your insights, provide feedback, or ask questions.

<summary>Contributing Guidelines</summary>

1. **Fork the Repository**: Start by forking the project repository to your local account.
2. **Clone Locally**: Clone the forked repository to your local machine using a git client.
   ```sh
   git clone ../
   ```
3. **Create a New Branch**: Always work on a new branch, giving it a descriptive name.
   ```sh
   git checkout -b new-feature-x
   ```
4. **Make Your Changes**: Develop and test your changes locally.
5. **Commit Your Changes**: Commit with a clear message describing your updates.
   ```sh
   git commit -m 'Implemented new feature x.'
   ```
6. **Push to local**: Push the changes to your forked repository.
   ```sh
   git push origin new-feature-x
   ```
7. **Submit a Pull Request**: Create a PR against the original project repository. Clearly describe the changes and
   their motivations.
8. **Review**: Once your PR is reviewed and approved, it will be merged into the main branch. Congratulations on your
   contribution!


<summary>Contributor Graph</summary>
<br>
<p style="text-align:center">
   <a href="https://local{//}graphs/contributors">
      <img src="https://contrib.rocks/image?repo=" alt="">
   </a>
</p>

---

## License

This project is protected under the [GNU Affero General Public License v3.0](https://choosealicense.com/licenses/agpl-3.0/)

---
