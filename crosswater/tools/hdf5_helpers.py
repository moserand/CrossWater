

def find_ids(h5_file):
    """Returns IDs from the group names.
    """
    ids = []
    for group in h5_file.walk_nodes('/', 'Group'):
        name = group._v_name  # pylint: disable=protected-access
        if name.startswith('catch_'):
            id_ = name.split('_')[1]
            ids.append(id_)
    ids.sort()
    return ids