# ipfs-ds-convert

> Datastore setup converter for go-ipfs

This tool is WIP and may damage your data. Make sure to backup first.

TODO:
- [x] Finish basic conversion code
- [x] package.json for gx
- [ ] Tests
  - [ ] CI (needs https://github.com/ipfs/go-ipfs/pull/4007, https://github.com/ipfs/go-ipfs/pull/3575)
  - [ ] Coverage > 80% or more
- [ ] Review
- [ ] Standard readme
- [ ] Revert on error
  - As a subcommand
- [ ] Optimize some standard cases
  - [ ] Don't copy directories when not needed
  - [ ] Detect renames
- [ ] Report progress
