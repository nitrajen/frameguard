from kedro.pipeline import Pipeline

from orders_pipeline.pipelines.processing import create_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    processing = create_pipeline()
    return {
        "__default__": processing,
        "processing": processing,
    }
