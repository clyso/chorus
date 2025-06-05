# Contributing to Chorus

Thank you for your interest in contributing to Chorus! We welcome contributions from the community, including code, documentation, bug reports, and feature suggestions.

## How to Contribute

### Code of Conduct
Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) to ensure a welcoming and inclusive environment for everyone.

### Getting Started

Clone the Repository: 
```shell
git clone https://github.com/clyso/chorus.git
cd chorus
```

Set Up Environment: Chorus is written in Go no other dependencies required.
To run tests:
```shell
go test ./...
```
Or:
```shell
make test
```

Check [README.md](./README.md) and [Makefile](./Makefile) for details.

### Submitting Changes
1. **Create an Issue**: For bugs or feature requests, open a GitHub issue to discuss your idea before submitting a pull request (PR). Small fixes can go without corresponding issue.
2. **Fork and Branch**: Fork the repository and create a feature branch (`git checkout -b your-feature-name`).
3. **Sign Off Commits**: Chorus uses the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). Add a `Signed-off-by: Your Name <your.email@example.com>` line to every commit:
   ```
   git commit -s -m "Your commit message"
   ```
5. **Submit a Pull Request**: Push your branch to your fork and submit a PR against the main branch of clyso/chorus

### Reporting Bugs

1. Open a GitHub issue with a clear title and description.
2. Include:
  - Expected vs. actual behavior.
  - Steps to reproduce (including code, if applicable).
  - Environment details (OS, Chorus version, S3 vendor, etc.).

Use the bug report template to ensure clarity.

### Proposing Features

1. Open a GitHub issue with a descriptive title.
2. Explain the proposed feature, why it’s needed, and how it aligns with Chorus’s goals (e.g., S3 compatibility, performance, or usability).
3. Provide a step-by-step description and any alternatives considered.
4. If applicable: explain how this feature changes current behaviour and how it affects backwards-compatibility.

## Legal Notes
By contributing, you agree to license your work under the [Apache 2.0 License](LICENSE). The DCO ensures your contributions are yours to submit and properly licensed. Significant contributions (e.g., new modules) may include a copyright notice in the format: `Copyright [Year] [Your Name/Company]`, consistent with existing headers.

### Questions?
Reach out via GitHub issues or email <info@clyso.com>.

Thank you for helping make Chorus better! 🚀
