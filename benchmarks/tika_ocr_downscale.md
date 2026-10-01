# OCR recall at full resolution versus a downscaled rendition

Task 2 of `docs/superpowers/plans/2026-09-30-tika-bounded-rendition.md`.
Measured 2026-10-01 against `apache/tika:3.2.3.0-full`, container limit
4 GiB so the full-resolution baseline completes.

The design sends a downscaled rendition to Tika instead of the original.
That is strictly worse for OCR than the original, and the spec asserted
the loss was acceptable without evidence. This measures it.

## Method

A photograph cannot answer the question, because it has no text. The
fixture is a dense page of **known** text, so recall is countable rather
than eyeballed:

- A0 at 300 dpi, 9933x14043, DejaVuSerif at 42 px, 210 lines
- ground truth: 300 occurrences of `Einlagezahl 412`, 3900 words,
  31199 characters
- renditions produced with Pillow LANCZOS, JPEG quality 85
- `PUT /tika`, `Accept: text/plain`, phrase recall and multiset word
  recall computed against the rendered ground truth

`glyph px` is the rendered cap height of the 42 px font at that
resolution, which is the quantity Tesseract actually cares about.

## Result at pgthumbor's real cap

pgthumbor caps `max(image.size)`, the **long** edge, so an A0 portrait
page becomes 2829x4000 and not 4000x5655.

| rendition | size | MP | glyph px | phrase recall | word recall |
|---|---|---|---|---|---|
| original | 9933x14043 | 139.5 | 42.0 | 100.0% | 100.0% |
| pgthumbor ceiling, 8000 px | 5659x8000 | 45.3 | 23.9 | **76.0%** | 94.1% |
| **pgthumbor default, 4000 px** | 2829x4000 | 11.3 | 12.0 | **97.0%** | **98.6%** |

## The full curve, by long edge

| long edge | MP | glyph px | chars | phrase recall | word recall |
|---|---|---|---|---|---|
| 9933 | 139.5 | 42.0 | 31199 | 100.0% | 100.0% |
| 6000 | 50.9 | 25.4 | 31587 | 70.7% | 87.4% |
| 4000 | 22.6 | 16.9 | 31199 | 100.0% | 100.0% |
| 3000 | 12.7 | 12.7 | 31185 | 92.0% | 99.0% |
| 2000 | 5.7 | 8.5 | 29729 | 25.7% | 50.9% |
| 1500 | 3.2 | 6.3 | 195 | 0.0% | 0.0% |

(These were produced by capping the *width*, so their long edge is larger
than the number in the first column. They remain useful as a glyph-height
series, which is the axis that matters.)

## Three conclusions

**1. The default cap is vindicated. Keep 16000000.**

`PGCATALOG_TIKA_MAX_IMAGE_PIXELS = 16000000` corresponds to pgthumbor's
4000 px edge cap, since a long edge of 4000 bounds any image at 16 MP.
The measured cost at that cap is 3 percentage points of phrase recall and
1.4 of word recall on a dense A0 page. That is a real but small loss, and
it buys not being OOM-killed. No change to the default.

**2. Resolution is not monotonic, so "bigger is safer" is false.**

Two independent samples in the 24-25 px glyph band did markedly *worse*
than both higher and lower resolutions: 25.4 px scored 70.7% and 23.9 px
scored 76.0%, while 12.0 px scored 97.0% and 42.0 px scored 100%. The
6000 px rendition also produced *more* characters than the ground truth,
31587 against 31199, which is the signature of misrecognition splitting
words rather than of missing text.

Reproduced twice, so it is not noise. The plausible cause is Tesseract's
own rescaling toward its preferred x-height interacting badly with certain
input scale factors.

**This retracts a recommendation in the spec.** Component 4 said that if
recall proved poor the response was to "raise the default cap and the Tika
memory limit together". Measurement says raising the cap toward
pgthumbor's 8000 px ceiling would make OCR **worse**, at 76.0% against
97.0%, while also costing four times the pixels. That sentence is wrong
and has been removed.

**3. The guard is a proxy, and its accuracy is content-dependent.**

The collapse is governed by glyph height, not by image dimensions: 12.7 px
still gives 92%, 8.5 px gives 25.7%, 6.3 px gives nothing. Glyph height
depends on the document's own layout, so the same 4000 px cap that costs
3% on this 42 px page would destroy a plan sheet whose annotations are
rendered half that size.

So a pixel cap cannot promise recall, only bounded memory. The honest
framing for the documentation is that an oversized image is indexed from a
bounded rendition, that this is lossy for text-bearing images, and that
`PGCATALOG_TIKA_MAX_IMAGE_PIXELS` is the knob for a site whose scans carry
small text — with the warning from conclusion 2 that raising it is not
reliably an improvement and should be measured on that site's own
material.

## Reproducing

Fixture generation and the OCR loop are in the appendix of
`docs/superpowers/specs/2026-09-30-tika-bounded-rendition-design.md`; the
only difference here is `--memory=4g` and `-full`, and that the rendition
sizes are computed from `max(W, H)` to match pgthumbor.
