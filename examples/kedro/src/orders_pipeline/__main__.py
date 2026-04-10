from kedro.framework.project import configure_project

configure_project("orders_pipeline")

from kedro.framework.session import KedroSession

with KedroSession.create() as session:
    session.run()
