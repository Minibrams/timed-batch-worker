set -e

rm -rf dist
rm -rf build

python -m build --sdist
python -m build --wheel

twine check dist/*
twine upload dist/*