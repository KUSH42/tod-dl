# TOD-DL

TOD-DL is the source repository for the Tor Onion Dump Downloader. It contains
the downloader, monitor, provenance verifier, tests, fixtures, and design
specifications.

The repository does not track acquired evidence, derived content, download
state, or Python virtual environments. Use `requirements-downloader.txt` and
`requirements-monitor.txt` to create local environments.

Run the downloader tests with:

```bash
python3 -m unittest -v test_download_priority.py test_monitor_priority.py
```
