"""Omnigent execution adapter (architecture §5, design task 3).

* :mod:`.adapter` - :class:`~.adapter.OmnigentExecutionAdapter`, the ``OmnigentAdapter`` port.
* :mod:`.rest` - explicit JSON REST transport, write classification, pagination, SSE.
* :mod:`.tree` - recursive, archive-inclusive stage-tree scans.
* :mod:`.policies` - github/CEL/cost policy specs and generation replacement.
* :mod:`.observe` - stream/snapshot normalization into core observation events.
* :mod:`.activity` - union active-time estimation with conservative upper bound.
* :mod:`.outcomes` - adapter outcome -> observation events for the executor.
* :mod:`.directory` - adapter-side context ports (dispatch tuple, texts, own-send ledger).
"""
