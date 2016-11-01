# Two Sigma Branches

We maintain additional branches as follows:

* 1.6.2-cook - This is a branch of Spark with Cook support added.
* 1.6.2-ts - This branch is based off of the 1.6.2-cook branch and has
  TS related backports applied for internal usage.

We will only base branches off of tagged released versions of Spark and therefore
the underlying branches will not change.

# Updating Cook Support

As new Spark versions are released we will maintain/update cook support on corresponding
branches in this repo with "-cook" or "-ts" suffixes (eg. "2.0.1-cook", "2.0.1-ts").
This will be done under a best effort basis by the TS team.

The process for upgrading a cook branch is to checkout the corresponding Spark release tag (eg. "v2.0.1")
and then rebase the previous Cook version's changes on to that version. Fix any compilation/correctness
issues and then push.

# Applying new Cook changes.

All new changes (except version specific bug fixes) for cook support should be applied directly to the cook
related branch in the latest version supported. Please submit a pull request with these changes and someone
with merge privileges will review your change.

If necessary, they will then be backported to the previous versions as required.

# TS Related Branches

These will be based off of the corresponding cook branch and will include cherry picked backports
as required for TS usage. **If the corresponding cook branch changes, it is expected that we will rebase
the TS branch at the same time.**

When we move to a new Spark version internally, we should always have a "-ts" branch to represent that.
At the bare minimum this will contain all of the cook related patches, but if additional backports
are necessary we can perform those as necessary. By always having a TS branch we can have one reference
point to the code we have deployed internally, without worrying about whether it's the "-cook" or "-ts"
branch at any point in time.

