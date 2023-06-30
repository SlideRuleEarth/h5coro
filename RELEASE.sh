#!/usr/bin/sh

#
# Typical version identifier is x.y.z, e.g. 1.0.4
# the version number is then prepended with 'v' for
# the tags annotation in git.
#
VERSION=$1
if git tag -l | grep -w $VERSION; then
    echo Git tag already exists
	exit 1
fi

#
# Update version in local repository
#
echo $VERSION > version.txt
git add version.txt
git commit -m "Version $VERSION"

#
# Create tag and release
#
git tag -a $VERSION -m "version $VERSION"
git push --tags && git push
gh release create $VERSION -t $VERSION
