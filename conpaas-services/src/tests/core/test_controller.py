def test_create_nodes(controller):
    '''Creating nodes'''
    if controller is not None:
        assert controller.create_nodes(2, lambda ip, port: True, None)


def test_delete_nodes(controller):
    '''Deleting nodes'''
    if controller is not None:
        nodes_created = controller.create_nodes(2, lambda ip, port: True, None)
        assert controller.delete_nodes(nodes_created)


def test_multiple_cloud_providers(controller):
    '''Testing if we can store multiple clouds'''
    if controller is not None:
        assert controller._Controller__default_cloud is not None
        assert controller._Controller__available_clouds is not None
