# FactCast 

#### is a 'good enough' event store using PostgreSQL for persistence, and offers REST and GRPC interfaces.

**This project is not yet ready for primetime**

It is not yet released, the API may change, the documentation is incomplete.

[![CircleCI](https://circleci.com/gh/Mercateo/factcast.svg?style=shield)](https://circleci.com/gh/Mercateo/factcast)
[![codecov](https://codecov.io/gh/Mercateo/factcast/branch/master/graph/badge.svg)](https://codecov.io/gh/Mercateo/factcast)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/dd5921cfeb81482db72fa8d9df68048f)](https://www.codacy.com/app/uwe/factcast?utm_source=github.com&utm_medium=referral&utm_content=uweschaefer/factcast&utm_campaign=badger)
[![CodeFactor](https://www.codefactor.io/repository/github/mercateo/factcast/badge)](https://www.codefactor.io/repository/github/mercateo/factcast)
[![MavenCentral](https://img.shields.io/maven-central/v/org.factcast/factcast.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.factcast%22)
<a href="https://www.apache.org/licenses/LICENSE-2.0">
    <img class="inline" src="https://img.shields.io/badge/license-ASL2-green.svg?style=flat">
</a>
[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=Mercateo/factcast)](https://dependabot.com)
[![DepShield Badge](https://depshield.sonatype.org/badges/Mercateo/factcast/depshield.svg)](https://depshield.github.io)
... under active development.

# The Problem at hand

In a micro-service world, teams choose their own tools of trade. This is a very important benefit of using Microservices in the first place, and you do not want to mess with this principle. However, where Subsystems communicate with each other (most likely crossing those team borders) you need some common ground. Event Sourcing is a great pattern here (as well as within those subsystems) because of the decoupling effect of its use.

So, what is needed is some technical solution, that everyone can easily agree on, because it forces as little technical dependencies on the clients as possible.
REST and similar technological choices provide this solution, and if spiced with streaming, we have all we need. Oh and one thing: Whatever solution we choose to store and stream forward needs to be failure tolerant, somewhat scalable and should pose minimal operational complexity and overhead to an existing system.

This is where some of the above solutions pose a possible problem:

While all of them are most probably great, when it comes to clustering, backup, data-/application-management and fail-over, none of these are trivial problems and most of them bring their own (certainly great) solution.

Gee, i wish there was a solution, that is flexible, platform neutral and could be operated at scale with what we already know...

[Read more on factcast.org](https://factcast.org)

[Detailed changlelog](https://docs.factcast.org/changelog)

