from setuptools import setup

setup(name='ips_legacy_uplift',
      version='0.0.1',
      description='Legacy Uplift version of the IPS system',
      url='',
      author='Social Surveys',
      author_email='social.surveys@ons.gsi.gov.uk',
      license='MIT',
      packages=['ips_legacy_uplift'],
      install_requires=[
          'sas7bdat',
          'survey_support',
          'cx_Oracle',
          'pandas',
          'numpy'
      ],
      zip_safe=False)