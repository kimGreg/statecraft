---
name: arithmetic-review
description: Price a quote from its line items using the calculator tools.
allowed-tools: calc.add calc.multiply
---
# Pricing a quote

1. For each line item, multiply its quantity by its unit price with `calc__multiply`.
2. Add the line totals with `calc__add`; never add or multiply in your head.
3. Answer with one JSON object: {"total": <number>, "lines": <number of line items>}.
