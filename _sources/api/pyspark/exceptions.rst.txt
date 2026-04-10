Exceptions
==========

All frameguard exceptions inherit from ``DfTypesError``, so you can catch
everything with a single ``except DfTypesError`` if needed.

.. code-block:: python

   from frameguard.pyspark.exceptions import DfTypesError, SchemaValidationError

   try:
       ds.validate(OrderSchema)
   except SchemaValidationError as e:
       print(e.errors)        # list of field-level mismatches
       print(e.history)       # full schema history up to the failure

----

.. autoclass:: frameguard.pyspark.exceptions.DfTypesError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.SchemaValidationError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.TypeAnnotationError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.ColumnNotFoundError
   :members:
