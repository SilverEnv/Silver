---
name: linear
description:
  Use Symphony's injected linear_graphql tool for Linear issue state, comments,
  attachments, and workpad updates during unattended Silver runs.
---

# Linear

## Primary Tool

During Symphony app-server sessions, prefer the injected `linear_graphql` tool
when available:

```json
{
  "query": "query or mutation document",
  "variables": {}
}
```

Send one GraphQL operation per call and treat top-level `errors` as failures.

## Common Queries

Read an issue by key:

```graphql
query IssueByKey($key: String!) {
  issue(id: $key) {
    id
    identifier
    title
    url
    description
    state { id name type }
    project { id name }
    team {
      id
      states { nodes { id name type } }
    }
    comments { nodes { id body resolvedAt } }
    attachments { nodes { id title url sourceType } }
  }
}
```

Create a workpad comment:

```graphql
mutation CreateComment($issueId: String!, $body: String!) {
  commentCreate(input: { issueId: $issueId, body: $body }) {
    success
    comment { id url }
  }
}
```

Update a workpad comment:

```graphql
mutation UpdateComment($id: String!, $body: String!) {
  commentUpdate(id: $id, input: { body: $body }) {
    success
    comment { id body }
  }
}
```

Move an issue to a state:

```graphql
mutation MoveIssue($id: String!, $stateId: String!) {
  issueUpdate(id: $id, input: { stateId: $stateId }) {
    success
    issue { id state { id name } }
  }
}
```

## Workpad Rule

Use one persistent comment headed `## Codex Workpad` for plan, acceptance
criteria, validation, notes, and handoff. Do not create separate progress
comments unless tool limitations require it.
