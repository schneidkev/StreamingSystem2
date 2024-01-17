import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.20"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}
val beamVersion = "2.53.0"

dependencies {
    testImplementation(kotlin("test"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.3")
    implementation("org.apache.kafka:kafka_2.13:3.6.0")
    implementation("org.apache.zookeeper:zookeeper:3.9.1")
    implementation("org.apache.logging.log4j:log4j-core:2.21.1")
    implementation("org.apache.beam:beam-sdks-java-core:${beamVersion}")
    implementation("org.apache.beam:beam-runners-direct-java:${beamVersion}")
    implementation("org.apache.beam:beam-sdks-java-io-kafka:${beamVersion}")

}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "19"
}

application {
    mainClass.set("MainKt")
}