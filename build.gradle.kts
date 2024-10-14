plugins {
    `idea`
    `java`
    `java-library`
    `java-test-fixtures`
    `maven-publish`
}

group = "io.spbx"
version = "0.1.0"

tasks.wrapper {
    gradleVersion = "8.10"
    jarFile = projectDir.resolve("gradle/wrapper/gradle-wrapper.jar")
    scriptFile = projectDir.resolve("gradle/wrapper/gradlew")
}

idea {
    module {
        outputDir = buildDir.resolve("idea/main")
        testOutputDir = buildDir.resolve("idea/test")
        isDownloadJavadoc = false
        isDownloadSources = true
    }
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

private val main by sourceSets
private val tutorial by sourceSets.creating

dependencies {
    "tutorialCompileOnly"(main.output)
    "tutorialCompileOnly"("org.jetbrains:annotations:24.1.0")
    "tutorialImplementation"("com.github.maxim5:java-basics:0.3.0")
    "tutorialRuntimeOnly"("com.google.flogger:flogger-log4j2-backend:0.8")
}

dependencies {
    api("com.google.guava:guava:33.2.0-jre")
    api("com.github.maxim5:java-basics:0.3.0")
    compileOnly("org.jetbrains:annotations:24.1.0")
    implementation("com.google.flogger:flogger:0.8")
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testCompileOnly("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")

    testCompileOnly("org.jetbrains:annotations:24.1.0")
    testImplementation("com.github.maxim5:java-basics:0.3.0:test-fixtures")
    testRuntimeOnly("com.google.flogger:flogger-log4j2-backend:0.8")

    testImplementation("com.google.truth:truth:1.4.2")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("com.google.flogger:flogger:0.8")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Jar> {
    from(main.output)

    manifest {}
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = "bigqueue"
            version = project.version.toString()
            from(components["java"])
        }
    }
}
