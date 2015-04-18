# Description
------
Albacore is a general framework for business logic development. It concerns about phase in whole lifecycle of an implementation of complex business logic, and is composed by components include:
##core
Basic framework, base classes and utils for albacore depended by other components. Including:
- exceptions
- basic database abstraction
- basic utils
	- generic
	- reflection
	- security & encrypt (encoding/decoding)
- object abstraction
It's minimum set covered requirement of a business logic development lifecycle.

##frame
Abstracting hiberarchy of a whole bizlogic into layer:
- Facade: providing interface between components of bizlogic.
- Service: implementation of actual bizlogic.
- DAO: encapsulate persistence operation like jdbc, cache, no-sql and so on, handle transactions and performances.

##orm
Implementation of relation database bizlogic, include mapping, transaction,
pooling, caching, and so on. It's build on Mybatis.

##orm-nosql
Extension of ORM, provide no-sql persistence operation with similar style code.

##utils
Provide lots of other utils for convenience.

##cache
> To be implemented, a framework includes cache providers and simulate key-value style persistence operations.

##test
> To be upgrade to version 2.X, which provides common usecase base class based on JUnit, and test suite assembly/running automatically from test cases scan in classpath.
