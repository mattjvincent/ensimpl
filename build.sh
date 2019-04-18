set -ex

# SET THE FOLLOWING VARIABLES
# docker hub username
USERNAME=mattjvincent
# image name
IMAGE=ensimpl

version=`cat VERSION`
echo "version: $version"

docker build -t $USERNAME/$IMAGE:$version .




