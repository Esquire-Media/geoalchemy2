"""This module defines specific functions for MSSQL dialect."""
from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import check_management
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry

_POSSIBLE_TYPES = [
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
]


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with MSSQL dialect."""
    if not isinstance(column_info.get("type"), (Geometry, NullType)):
        return

    column_name = column_info.get("name")

    # Check geometry type, SRID and if the column is nullable
    geometry_type_query = f"""
        SELECT 
            DATA_TYPE, 
            (SELECT TOP(1) [{column_name}].STSrid FROM {f"[{table.schema}]." if table.schema else ""}[{table.name}]) AS SRS_ID, 
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE 
            {f"TABLE_SCHEMA = '{table.schema}' and" if table.schema else ""}
            TABLE_NAME = '{table.name}' 
            and 
            COLUMN_NAME = '{column_name}'
    """
    geometry_type, srid, nullable_str = inspector.bind.execute(
        text(geometry_type_query)
    ).one()
    is_nullable = str(nullable_str).lower() == "yes"

    if geometry_type not in _POSSIBLE_TYPES:
        return

    # Check if the column has spatial index
    has_index_query = f"""
        SELECT DISTINCT
            INDEX_TYPE
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE 
            {f"TABLE_SCHEMA = '{table.schema}' and" if table.schema else ""}
            TABLE_NAME = '{table.name}' 
            and 
            COLUMN_NAME = '{column_name}'
    """
    spatial_index_res = inspector.bind.execute(text(has_index_query)).scalar()
    spatial_index = str(spatial_index_res).lower() == "spatial"

    # Set attributes
    column_info["type"] = Geometry(
        geometry_type=geometry_type.upper(),
        srid=srid,
        spatial_index=spatial_index,
        nullable=is_nullable,
        _spatial_index_reflected=True,
    )

_MSSQL_FUNCTIONS = {
    "ST_AsBinary": "STAsBinary",
    "ST_AsEWKB": "STAsBinary",
}


def _compiles_mssql(cls, fn):
    def _compile_mssql(element, compiler, **kw):
        return f"({compiler.process(element.clauses, **kw)}).{fn}()"

    compiles(getattr(functions, cls), "mssql")(_compile_mssql)


def register_mssql_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mssql_function_name_1",
                    "function_name_2": "mssql_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mssql(cls, fn)


register_mssql_mapping(_MSSQL_FUNCTIONS)


def _compile_GeomFromText_MsSql(element, compiler, **kw):
    element.identifier = "geography::STGeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({}, 4326)".format(element.identifier, compiled)


def _compile_GeomFromWKB_MsSql(element, compiler, **kw):
    element.identifier = "geography::STGeomFromWKB"
    wkb_data = list(element.clauses)[0].value
    if isinstance(wkb_data, memoryview):
        list(element.clauses)[0].value = wkb_data.tobytes()
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


@compiles(functions.ST_GeomFromText, "mssql")
def _MSSQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MsSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mssql")
def _MSSQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MsSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mssql")
def _MSSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MsSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mssql")
def _MSSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MsSql(element, compiler, **kw)
