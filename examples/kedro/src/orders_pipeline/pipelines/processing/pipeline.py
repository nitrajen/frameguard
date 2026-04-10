from kedro.pipeline import Pipeline, node, pipeline

from .nodes import enrich_orders, summarise_by_customer


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=enrich_orders,
            inputs="raw_orders",
            outputs="enriched_orders",
            name="enrich_orders_node",
        ),
        node(
            func=summarise_by_customer,
            inputs="enriched_orders",
            outputs="customer_summary",
            name="summarise_node",
        ),
    ])
