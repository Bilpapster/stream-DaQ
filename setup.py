from setuptools import setup, find_packages

setup(
    name='streamdaq',
    version='0.1.5',
    author='DELab Aristotle University of Thessaloniki, Vassilis Papastergios, Apostolos Giannoulidis, Anastasios Gounaris',
    author_email='bilpapster@gmail.com',
    description='Plug-and-play real-time quality monitoring for data streams!',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Bilpapster/stream-DaQ',
    packages=find_packages(),
    install_requires=[
        'pathway==0.21.1',
        'matplotlib==3.10.1',
        'python-dateutil==2.9.0.post0',
        'datasketch~=1.6.5',
        'datasketches~=5.1.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    license="MIT License",
    python_requires='>=3.10',
)