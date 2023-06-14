import sys
from pathlib import Path


def test_example_flow():
    """
    This test just runs the example pipeline that we provide in example/run_pipeline.py
    """

    example_path = str(Path(__file__).parent / ".." / ".." / "example")
    sys.path.insert(0, example_path)
    from example.run_pipeline import main

    main()
