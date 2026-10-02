# Real Apache Tika response fixtures, for two majors

Captured for Task 3 of
`docs/superpowers/plans/2026-09-30-tika-bounded-rendition.md`. There are
two sets because **the two majors differ in ways that break code**, and a
single-major fixture set is how that went unnoticed for a week.

The `rmeta_*` payloads arrive with the `/rmeta/text` change that consumes
them; this change carries the capability payloads, which are the evidence
for the OCR-probe decision in the design doc.

## Provenance

| File | Source | Captured |
|---|---|---|
| `parsers_details_3_2_3_stock.json` | `apache/tika:3.2.3.0` | 2026-10-01 |
| `parsers_details_3_2_3_full.json` | `apache/tika:3.2.3.0-full` | 2026-10-01 |
| `rmeta_3_2_3_compound_zip.json` | `apache/tika:3.2.3.0` | 2026-10-01 |
| `rmeta_3_2_3_image_exif.json` | `apache/tika:3.2.3.0` | 2026-10-01 |
| `parsers_details_4_1_0_stock.json` | `apache/tika@sha256:06bcdbd0…`, the digest aaf-prod runs | 2026-10-02 |
| `parsers_details_4_1_0_full.json` | `apache/tika:4.1.0-full` | 2026-10-02 |
| `rmeta_4_1_0_compound_zip.json` | the production digest | 2026-10-02 |
| `rmeta_4_1_0_image_exif.json` | the production digest | 2026-10-02 |

The 3.x set was taken at a 1 GiB limit, which is what #222 reported. The
4.1.0 set was taken at 2 GiB, the limit aaf-prod actually runs. Payloads
are the same two documents throughout: a ZIP holding `vertrag.txt` and
`anhang.txt`, and a 4000x3000 JPEG carrying an EXIF `ImageDescription`.
Generators are in the appendix of the design doc.

## What changed between the majors, and what it cost

**`X-TIKA:content` became `tk:content`.** Also `resourceName` →
`tk:resource-name`, `X-TIKA:parse_time_millis` → `tk:parse-time-millis`.
Dublin Core keys and `Content-Type` are unchanged. Code reading only the
3.x name returns **empty text for every document** against 4.x, which
reads as "this file has no text" rather than as a bug. `tika_rmeta.py`
tries both names per entry, and
`test_both_majors_from_their_real_fixtures_agree` pins it.

**The `image/ocr-*` OCR signal is gone.** On 3.2.3, stock advertised 0 and
`-full` advertised 8 of those pseudo-types, which made OCR availability
readable from `/parsers/details`. On 4.1.0 **both images advertise 0**,
and more than that they are semantically identical: 89 parser classes, the
same `supportedTypes`, 279 media types, differing only in key ordering.
Yet 4.1.0 `-full` really does OCR, with Tesseract 5.5.0, returning
`TIKAOCR` for the probe image while stock returns nothing.

So there is no introspection-based OCR probe on 4.x. `tika_policy` probes
by **behaviour** instead, sending
`src/plone/pgcatalog/assets/ocr_probe.png` through the real parser:
measured 0.02 s and empty against stock, 0.17 s and `TIKAOCR` against
`-full`. `ocr_types()` survives for diagnostics only, and
`test_4_1_0_payloads_carry_no_ocr_signal` is what documents why the
decision cannot use it.

## Invariants the tests assert

```
3.2.3: stock 0 image/ocr-* types, full 8;  264 vs 275 supported types
4.1.0: stock 0 image/ocr-* types, full 0;  279 vs 279, semantically equal
both : the compound ZIP's container entry holds only the file names,
        so concatenating entries does not double-count
both : the image's dc:description carries the EXIF ImageDescription
        with no OCR involved
```

## Updating

Only alongside a deliberate Tika version change, and then capture
**every** variant in that same change: stock and `-full`, for each major
you intend to support. Keeping one major only is what let a production
drift from 3.x to 4.1.0 invalidate a week of measurements silently.

If a future Tika restores an `image/ocr-*` family, that is a real
behaviour change and not a fixture to refresh quietly:
`test_3_2_3_payloads_did_carry_the_signal` exists so the restoration gets
noticed rather than assumed.
