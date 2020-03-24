from setuptools import setup

setup(name='druid_exporter',
      version='0.6',
      description='Prometheus exporter for Druid',
      url='https://github.com/wikimedia/operations-software-druid_exporter',
      author='Luca Toscano',
      author_email='ltoscano@wikimedia.org',
      license='Apache License, Version 2.0',
      packages=['druid_exporter'],
      package_data={'druid_exporter': ['supported_metrics.json']},
      include_package_data=True,
      install_requires=[
          'prometheus-client',
      ],
      entry_points={
          'console_scripts': [
              'druid_exporter = druid_exporter.exporter:main'
          ]
      },
      )
