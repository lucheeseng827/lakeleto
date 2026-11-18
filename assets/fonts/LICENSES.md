# Vendored fonts

Both faces are self-hosted rather than loaded from a CDN. Lakeleto's air-gapped /
self-contained constraint (PRODUCT.md, constraint 1) forbids any runtime network request,
and a font request would also hand a third party every visitor's IP address.

Only the **Latin** subsets are vendored. Shippori Mincho is a CJK family that Google splits
into ~122 subsets per weight; shipping the whole family would cost several megabytes for
glyphs this page never sets.

| File | Family | Weight | License |
|------|--------|--------|---------|
| `shippori-mincho-latin-400.woff2` | Shippori Mincho | 400 | SIL Open Font License 1.1 |
| `shippori-mincho-latin-800.woff2` | Shippori Mincho | 800 | SIL Open Font License 1.1 |
| `jetbrains-mono-latin-400.woff2` | JetBrains Mono | 400 | SIL Open Font License 1.1 |
| `jetbrains-mono-latin-600.woff2` | JetBrains Mono | 600 | SIL Open Font License 1.1 |

**Shippori Mincho** — Copyright 2020 The Shippori Mincho Project Authors
(https://github.com/fontdasu/ShipporiMincho). Chosen because it is a mincho drawn from
Edo-era printing, which is the lettering tradition of the page's visual world. It is the
display voice.

**JetBrains Mono** — Copyright 2020 The JetBrains Mono Project Authors
(https://github.com/JetBrains/JetBrainsMono). Already a Lakeleto brand commitment: it is the
app UI's monospace face, self-hosted there via `@fontsource`. Copied from
`rust_modules/lab/module_72/frontend/dist/assets/`. Used here only for data, paths, and code,
never as a display voice.

The SIL Open Font License 1.1 permits redistribution of the font files, bundled or
standalone, provided the copyright notice and license travel with them and the fonts are not
sold on their own. Full text: https://openfontlicense.org
