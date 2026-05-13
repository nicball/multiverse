You are a proficient haskell engineer working on Multiverse. Prefer clean algebraic core types and explicit control flow while keeping impure code small and composable. Avoid unnecessary abstractions or overcomplicated effect systems.

# The Core

Multiverse is a event center for bridging between various chat platforms. The core data is called timeline as described in `agent.hs` as pseudo haskell. It is an set of events ordered by submission. The ordering of events is internal to the timeline and exposed through certain APIs.

Each entity (message, user, room, blob, etc) is identified by its creation event id. There could be various state changing events for the entity. The timeline provides APIs for querying the calculated state.

Each event has a platform key which describes the origin platform event id. The platform key must be unique. Each event has an ID which is a cryptographic hash from the event content. Therefore the ID is also unique and provide idempotency for submission.

The timeline is persistent through sqlite. It may has additional tables to make state query more efficient. The timeline is a typeclass which the sqlite backend provides an instance with.

# The Bridges

Each platform, i.e. Telegram/Matrix/QQ, bridge maintains its own two way mapping of platform entities/events to its timeline counterparts. This mapping is also persistent separately.

Each bridge has two parts: Observer and Reflector.

The observer listens to platform events and submit them to the timeline, updating the mapping.

The reflector listens to the timeline and relays the events to the platform according to the mapping.

Puppeting is not supported. A relaying account must be provided for the platform. The account is used for observing and reflecting. The matrix bridge could use MSC4144 to make relayed messages look better. While on other platforms the sender may be specified in text.

The interaction between bridges and timeline is made robust through idempotency and persistency so no messages are lost upon error or restart.

Each bridge provides options to specify initial room mappings. It can create timeline rooms if not already exists for convenience.

The telegram bridge uses bot api. The QQ bridge is not planned for now.

# Project Rules

Use nix flake to build the project or find tools. If dependencies are modified, reflect it in package.nix with cabal2nix.

## Language style and haskell extensions

Use record dot syntax and disable generation of old style record access functions.
Use block arguments to reduce the number of $'s.
Use 'qualified as' suffix form.
