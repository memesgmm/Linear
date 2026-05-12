#!/bin/bash
set -e

echo "Building Legacy (1.21.1) JAR..."
./gradlew clean jar -PbuildTarget=legacy
mv build/libs/*.jar ./build/libs/linear-legacy.jar

echo "Building Modern (26.1.2) JAR..."
./gradlew clean jar -PbuildTarget=modern
mv build/libs/*.jar ./build/libs/linear-modern.jar

echo "Build complete."
ls -l build/libs/linear-*.jar
