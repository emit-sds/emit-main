from emit_workflow import file_manager

def test_file_manager():
    fm = file_manager.FileManager(
        acquisition_id="emit20200101t000000",
        config_path="test_config.json"
    )

    fm.touch_path(fm.l1a["raw_img"])
    assert fm.path_exists(fm.l1a["raw_img"])

test_file_manager()