"""Run the package entrypoint for `python -m app`.

Docker starts the ECS task with `python -m app`, which makes Python execute
this module. `SystemExit` converts the integer returned by `main()` into the
container process exit code observed by ECS.
"""

from app.cli import main

raise SystemExit(main())
