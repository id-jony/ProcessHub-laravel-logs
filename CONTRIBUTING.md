# Contributing

Thanks for your interest in improving `processhub/laravel-logs`. This package
has a small surface and strict compatibility requirements — please read
before sending a PR.

## Scope

The package is a **delivery layer** for ProcessHub's ingest endpoints. New
features should either:
- Reduce setup friction for typical Laravel apps (less config, better defaults).
- Fill gaps in what Laravel emits as structured events (new listener).
- Harden failure modes (retry, fallback, rate-limit).

If you're proposing a new domain concept (metrics, traces, …) open an issue
first — ProcessHub's server-side needs to accept it before the client
packaging is meaningful.

## Development setup

```bash
git clone <repo>
cd laravel-logs
composer install
vendor/bin/phpunit
vendor/bin/phpstan analyse
```

## Tests

All PRs must include tests. We use [Orchestra Testbench](https://github.com/orchestral/testbench)
for a minimal Laravel bootstrap.

- **Unit** tests live in `tests/Unit/` — pure functions, no Laravel container.
- **Feature** tests in `tests/Feature/` — Testbench-backed, can use DI /
  config / facades.

We do **not** run integration tests against a live ProcessHub tenant in CI
(credentials would have to live in secrets). The install-doc says «run
`php artisan processhub:test`» for end-to-end sanity — that's the manual
check for maintainers as well.

## Compatibility matrix

We commit to supporting the matrix tested in CI:
- PHP 8.1 / 8.2 / 8.3
- Laravel 10.x / 11.x / 12.x

Dropping a row from the matrix is a **breaking** release and requires a
major version bump.

## Style

- PSR-12 (enforced implicitly by level-6 PHPStan).
- Short prose comments > verbose PHPDoc when the type info is already obvious
  from native types.
- No abbreviations in class names — `ProcessHubServiceProvider` not `PHSP`.

## Release process

1. Update `CHANGELOG.md` under `[Unreleased]` → new version section with date.
2. Tag: `git tag -a v0.2.0 -m "v0.2.0"` → `git push --tags`.
3. Packagist auto-picks the tag; `composer require processhub/laravel-logs:^0.2`
   in a test app to verify.
