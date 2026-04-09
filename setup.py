from setuptools import setup


package_name = "sensor_timing_viz"


setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="TODO",
    maintainer_email="todo@example.com",
    description="Standalone ROS 2 package for interactive sensor timing visualization from rosbag2 data.",
    license="Apache-2.0",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "sensor_timing_cli = sensor_timing_viz.cli:main",
            "sensor_timing_gui = sensor_timing_viz.gui:main",
        ],
    },
)
