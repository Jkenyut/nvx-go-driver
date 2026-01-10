# Contributing to nvx-go-driver

Thank you for your interest in contributing to **nvx-go-driver**! We welcome improvements, bug fixes, and new features.

## Getting Started

1.  **Fork** the repository.
2.  **Clone** your fork locally.
3.  **Install dependencies**:
    ```bash
    make deps
    ```

## Development Workflow

1.  Create a feature branch: `git checkout -b feature/my-new-feature`
2.  Make your changes.
3.  **Run tests**:
    ```bash
    make test
    ```
4.  Ensure code quality (lint):
    ```bash
    make lint
    ```
5.  Commit your changes: `git commit -m "feat: add new feature"`
6.  Push to your fork: `git push origin feature/my-new-feature`
7.  Open a **Pull Request**.

## Coding Standards

- Follow idiomatic Go guidelines (Effective Go).
- Ensure all public functions have GoDocs.
- Add unit tests for new functionality.
- Keep dependencies minimal.

## Reporting Issues

Please search existing issues before opening a new one. Include:
- Description of the issue
- Steps to reproduce
- Expected behavior
- Environment details (Go version, OS)
