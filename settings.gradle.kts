
rootProject.name = "StreamingSystem2"
include("src:main:java")
findProject(":src:main:java")?.name = "java"
include("Beam")
include("java")
include("untitled")
