# Real Apache Tika response fixtures

Captured 2026-10-01 for Task 3 of
`docs/superpowers/plans/2026-09-30-tika-bounded-rendition.md`. A fixture
whose origin is unknown is a fixture nobody dares update, so this records
exactly where each one came from.

## Provenance

| File | Source | Request |
|---|---|---|
| `parsers_details_stock.json` | `apache/tika:3.2.3.0` | `GET /parsers/details`, `Accept: application/json` |
| `parsers_details_full.json` | `apache/tika:3.2.3.0-full` | same |
| `rmeta_compound_zip.json` | `apache/tika:3.2.3.0` | `PUT /rmeta/text` of a two-file ZIP |
| `rmeta_image_exif.json` | `apache/tika:3.2.3.0` | `PUT /rmeta/text` of a 4000x3000 JPEG with EXIF |

Containers were run with `--memory=1g`, matching the production limit in
#222. The ZIP holds `vertrag.txt` and `anhang.txt`; the JPEG carries an
EXIF `ImageDescription`, `Artist`, `Copyright`, `Make` and `Model`. Both
generators are in the appendix of
`docs/superpowers/specs/2026-09-30-tika-bounded-rendition-design.md`.

## Invariants these fixtures exist to pin

Checked at capture time, and the unit tests assert the same things:

```
stock 0, full 8 image/ocr-* types
   image/ocr-bmp, image/ocr-gif, image/ocr-jp2, image/ocr-jpeg,
   image/ocr-jpx, image/ocr-png, image/ocr-tiff, image/ocr-x-portable-pixmap
   total supported types: stock 264, full 275
rmeta compound: 3 entries, container content='vertrag.txt\n\n\nanhang.txt'
rmeta image: dc:description='Sonnenuntergang am Attersee, Aufnahme vom Steg'
```

Three things follow, and each is load-bearing for the design:

1. **`image/ocr-*` is the only reliable OCR signal.** `image/jpeg` is
   claimed by `JpegParser` in *both* images and no media type has a
   different parser between them, so a plain capability query cannot tell
   an OCR deployment from a metadata-only one. Zero `image/ocr-*` in the
   stock image, eight in `-full`.
2. **Concatenating `/rmeta/text` entries does not double-count.** The
   container entry's content is the file *names* only; the two child
   entries hold the text. Word order does differ from `PUT /tika`, which
   interleaves each name with its content.
3. **An image yields text without any OCR.** `dc:description` comes from
   the EXIF `ImageDescription`, at metadata-only cost, which is why the
   design keeps image types in the pipeline instead of dropping them.

## Updating

Only alongside a deliberate Tika version bump, and then re-run the
capture for **both** images in the same change, because the stock-versus-
`-full` difference is the fixture's whole point. If a future Tika renames
the `image/ocr-*` family, that is a real behaviour change for
`tika_policy.probe_ocr` and not a fixture to quietly refresh: the
`PGCATALOG_TIKA_OCR` override exists for exactly that case.
