set -ex

# SET THE FOLLOWING VARIABLES
# docker hub username
USERNAME=mattjvincent
# image name
IMAGE=ensimpl_snps

docker build -t $USERNAME/$IMAGE:latest .



