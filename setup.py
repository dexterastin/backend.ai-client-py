from setuptools import setup, find_namespace_packages
from pathlib import Path
import re

setup_requires = [
    'setuptools>=41.2.0',
]
install_requires = [
    'backend.ai-cli~=0.2',
    'Click>=7.0',
    'PyYAML~=5.1.2',
    'appdirs~=1.4.3',
    'aiohttp~=3.6.2',
    'async_timeout~=3.0',  # to avoid pip10 resolver issue
    'attrs>=19.3',         # to avoid pip10 resolver issue
    'colorama~=0.4.3',
    'humanize>=0.5.1',
    'multidict~=4.7.3',
    'python-dateutil>=2.8',
    'tabulate>=0.8.6',
    'tqdm~=4.41',
    'yarl~=1.4.2',
]
build_requires = [
    'wheel>=0.33.6',
    'twine>=1.14.0',
]
test_requires = [
    'pytest~=5.3.2',
    'pytest-cov',
    'pytest-mock',
    'pytest-asyncio>=0.10.0',
    'aioresponses==0.6.1',
    'asynctest>=0.13; python_version<"3.8"',
    'codecov',
]
lint_requires = [
    'flake8>=3.7.8',
]
typecheck_requires = [
    'mypy>=0.760',
]
dev_requires = [
    'pytest-sugar>=0.9.1',
]
docs_requires = [
    'sphinx~=2.2',
    'sphinx-intl>=2.0',
    'sphinx_rtd_theme>=0.4.3',
    'sphinxcontrib-trio~=1.1.0',
    'sphinx-autodoc-typehints~=1.8.0',
    'pygments~=2.4',
]


def read_src_version():
    path = (Path(__file__).parent / 'src' /
            'ai' / 'backend' / 'client' / '__init__.py')
    src = path.read_text(encoding='utf-8')
    m = re.search(r"^__version__ = '([^']+)'$", src, re.MULTILINE)
    assert m is not None, 'Could not read the version information!'
    return m.group(1)


setup(
    name='backend.ai-client',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=read_src_version(),
    description='Backend.AI Client for Python',
    long_description=Path('README.rst').read_text(encoding='utf-8'),
    url='https://github.com/lablup/backend.ai-client-py',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],
    package_dir={'': 'src'},
    packages=find_namespace_packages(where='src', include='ai.backend.*'),
    python_requires='>=3.6',
    setup_requires=setup_requires,
    install_requires=install_requires,
    extras_require={
        'dev': dev_requires,
        'test': test_requires,
        'lint': lint_requires,
        'typecheck': typecheck_requires,
        'docs': docs_requires,
    },
    data_files=[],
    entry_points={
        'backendai_cli_v10': [
            '_ = ai.backend.client.cli:main',
        ]
    },
)
