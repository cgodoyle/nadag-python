# Changelog

All notable changes to this project will be documented in this file.

<!-- version list -->

## v0.1.0 (2026-04-28)

### Bug Fixes

- **models**: Correct harPrøveseriedel typo and add robust nested key extraction for sample hrefs
  ([`008849c`](https://github.com/cgodoyle/nadag-python/commit/008849c898083de750ee6444644cf72a51fcd53a))

- **robustness**: Replace fragile dict/index access with safe helpers across pipeline
  ([`7717787`](https://github.com/cgodoyle/nadag-python/commit/77177874f6be386b563274d5c3ffe5f02401e096))

### Features

- **audit**: Add API schema audit tool to detect field mismatches against live NADAG API
  ([`b594a56`](https://github.com/cgodoyle/nadag-python/commit/b594a56a9969c7dcb2a4ec9b1659b6e3a22c0f10))

### Refactoring

- Move shared type aliases to types.py to break circular import between data_models and utils
  ([`9a70385`](https://github.com/cgodoyle/nadag-python/commit/9a703851b1676cea921f4a257a6e404ce1285adf))


## v0.0.0 (2026-04-27)
