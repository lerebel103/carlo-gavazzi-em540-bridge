"""Allow running the package with ``python -m app``."""

import asyncio
from app.main import main

asyncio.run(main())
