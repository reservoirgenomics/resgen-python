import sys

_track_default_position = {
    "2d-rectangle-domains": "center",
    "bedlike": "top",
    "horizontal-bar": "top",
    "horizontal-chromosome-labels": "top",
    "chromosome-labels": "top",
    "horizontal-gene-annotations": "top",
    "horizontal-heatmap": "top",
    "horizontal-1d-heatmap": "top",
    "horizontal-line": "top",
    "horizontal-multivec": "top",
    "bar": "top",
    "chromosome-labels": "top",
    "gene-annotations": "top",
    "heatmap": "top",
    "1d-heatmap": "top",
    "line": "top",
    "horizontal-multivec": "top",
    "heatmap": "center",
    "left-axis": "left",
    "osm-tiles": "center",
    "top-axis": "top",
    "viewport-projection-center": "center",
    "viewport-projection-horizontal": "top",
}


_datatype_default_track = {
    "2d-rectangle-domains": "2d-rectangle-domains",
    "bedlike": "bedlike",
    "chromsizes": "horizontal-chromosome-labels",
    "gene-annotations": "horizontal-gene-annotations",
    "matrix": "heatmap",
    "vector": "horizontal-bar",
    "multivec": "horizontal-multivec",
}

def tracktype_default_position(tracktype: str):
    """
    Get the default track position for a track type.

    For example, default position for a heatmap is 'center'.
    If the provided track type has no known default position
    return None.

    Parameters
    ----------
    tracktype: str
        The track type to check

    Returns
    -------
    str:
        The default position
    """
    if tracktype in _track_default_position:
        return _track_default_position[tracktype]

    return None


def datatype_to_tracktype(datatype):
    """
    Infer a default track type from a data type. There can
    be other track types that can display a given data type.

    Parameters
    ----------
    datatype: str
        A datatype identifier (e.g. 'matrix')

    Returns
    -------
    str, str:
        A track type (e.g. 'heatmap') and position (e.g. 'top')

    """
    track_type = _datatype_default_track.get(datatype, None)
    position = _track_default_position.get(track_type, None)
    return track_type, position


def position_to_viewport_projection_type(position):
    if position == "center":
        track_type = "viewport-projection-center"
    elif position == "top" or position == "bottom":
        track_type = "viewport-projection-horizontal"
    elif position == "left" or position == "right":
        track_type = "viewport-projection-vertical"
    else:
        track_type = "viewport-projection"

    return track_type


def recommend_filetype(filename):
    ext = op.splitext(filename)
    if op.splitext(filename)[1] == ".bed":
        return "bedfile"
    if op.splitext(filename)[1] == ".bedpe":
        return "bedpe"


def recommend_datatype(filetype):
    if filetype == "bedfile":
        return "bedlike"


FILETYPES = {
    "cooler": {
        "description": "multi-resolution cooler file",
        "extensions": [".mcool"],
        "datatypes": ["matrix"],
    },
    "bigwig": {
        "description": "Genomics focused multi-resolution vector file",
        "extensions": [".bw", ".bigwig"],
        "datatypes": ["vector"],
    },
    "beddb": {
        "description": "SQLite-based multi-resolution annotation file",
        "extensions": [".beddb", ".multires.db"],
        "datatypes": ["bedlike", "gene-annotations"],
    },
    "hitile": {
        "description": "Multi-resolution vector file",
        "extensions": [".hitile"],
        "datatypes": ["vector"],
    },
    "time-interval-json": {
        "description": "Time interval notation",
        "extensions": [".htime"],
        "datatypes": ["time-interval"],
    },
}


def infer_filetype(filename):
    _, ext = op.splitext(filename)

    for filetype, meta in FILETYPES.items():
        if ext.lower() in meta["extensions"]:
            return filetype

    return None


def infer_datatype(filetype):
    if filetype in FILETYPES:
        return FILETYPES[filetype]["datatypes"][0]

    return None


def fill_filetype_and_datatype(filename, filetype=None, datatype=None):
    """
    If no filetype or datatype are provided, add them
    based on the given filename.
    Parameters:
    ----------
    filename: str
        The name of the file
    filetype: str
        The type of the file (can be None)
    datatype: str
        The datatype for the data in the file (can be None)
    Returns:
    --------
    (filetype, datatype): (str, str)
        Filled in filetype and datatype based on the given filename
    """
    if filetype is None:
        # no filetype provided, try a few common filetypes
        filetype = infer_filetype(filename)

        if filetype is None:
            recommended_filetype = recommend_filetype(filename)

            print(
                "Unknown filetype, please specify using the --filetype option",
                file=sys.stderr,
            )
            if recommended_filetype is not None:
                print(
                    "Based on the filename, you may want to try the filetype: {}".format(
                        recommended_filetype
                    )
                )

            return (None, None)

    if datatype is None:
        datatype = infer_datatype(filetype)

        if datatype is None:
            recommended_datatype = recommend_datatype(filetype)
            print(
                "Unknown datatype, please specify using the --datatype option",
                file=sys.stderr,
            )
            if recommended_datatype is not None:
                print(
                    "Based on the filetype, you may want to try the datatype: {}".format(
                        recommended_datatype
                    )
                )

    return filetype, datatype
