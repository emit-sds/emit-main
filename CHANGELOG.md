### Changelog

All notable changes to this project will be documented in this file. Dates are displayed in UTC.

#### [v1.6.22](https://github.com/emit-sds/emit-main/compare/v1.6.21...v1.6.22)

> 2 April 2025

* Julia 191 rollback by @pgbrodrick in https://github.com/emit-sds/emit-main/pull/53
* Merge main back into develop to resolve some commit issues by @winstonolson in https://github.com/emit-sds/emit-main/pull/55
* GHG and config updates by @adamchlus in https://github.com/emit-sds/emit-main/pull/57
* config change for tetracorder libraries - test only (not ops) by @pgbrodrick in https://github.com/emit-sds/emit-main/pull/58

#### [v1.6.21](https://github.com/emit-sds/emit-main/compare/v1.6.20...v1.6.21)

> 12 December 2024

- Julia 191 rollback [`#53`](https://github.com/emit-sds/emit-main/pull/53)
- Prepare for v1.6.21 release. Add new task runtimes script. [`0fe5082`](https://github.com/emit-sds/emit-main/commit/0fe5082332f3c239aa8bb5cdd2582feb1d0c45e5)
- Hotfix runtime bugs [`1f288fb`](https://github.com/emit-sds/emit-main/commit/1f288fb6eec874dfdddb8119634a8cb2d0e362ce)

#### [v1.6.20](https://github.com/emit-sds/emit-main/compare/v1.6.19...v1.6.20)

> 10 December 2024

- Merge develop into main for v1.6.20 [`#52`](https://github.com/emit-sds/emit-main/pull/52)
- small path modifications for new hardware [`#51`](https://github.com/emit-sds/emit-main/pull/51)
- Config updates for migration [`#50`](https://github.com/emit-sds/emit-main/pull/50)
- Use latest compile and plot metrics scripts. [`194a5cb`](https://github.com/emit-sds/emit-main/commit/194a5cbe7c345c7d3cf5f5ddcdaa2873723e4de1)
- Prep for 1.6.20 release - may need to change l1b config one more time [`25e3730`](https://github.com/emit-sds/emit-main/commit/25e3730007ca51c39841a4077a1e8b97d9cf2c36)
- Prepare for v1.6.20 release [`8182b5f`](https://github.com/emit-sds/emit-main/commit/8182b5f33b9f0bb1a973cf49d1b08acf243b84e8)

#### [v1.6.19](https://github.com/emit-sds/emit-main/compare/v1.6.18...v1.6.19)

> 1 October 2024

- Merge develop to main for v1.6.19 [`#49`](https://github.com/emit-sds/emit-main/pull/49)
- Use S3 for delivery staging [`#48`](https://github.com/emit-sds/emit-main/pull/48)
- Patch in updates from ops environment [`264de52`](https://github.com/emit-sds/emit-main/commit/264de528cd042aa01b5fb58135387daf3792c932)
- Switch to S3 delivery [`2f8a11f`](https://github.com/emit-sds/emit-main/commit/2f8a11f41cdf7cfec7fe532c5657464a467246b4)
- Prepare for 1.6.19 [`c0d34a2`](https://github.com/emit-sds/emit-main/commit/c0d34a26ee52bf3ec12f63bd80ea69c72e7f60b1)

#### [v1.6.18](https://github.com/emit-sds/emit-main/compare/v1.6.17...v1.6.18)

> 1 September 2023

- Merge develop into main for v1.6.18 [`#47`](https://github.com/emit-sds/emit-main/pull/47)
- Prep for 1.6.18 release and clean up PEP8 [`8d3708f`](https://github.com/emit-sds/emit-main/commit/8d3708f48cf06a3d78b51c01e144bec9425b9b4e)
- Don't add ffupdate paths to rdn header. [`0c242e0`](https://github.com/emit-sds/emit-main/commit/0c242e03fb1cec298e82f465374e982cf6b4c961)
- Don't compress on rsync. Check BAD start/stop time. Use projection on ff_update query. Update status report. [`9f5d64d`](https://github.com/emit-sds/emit-main/commit/9f5d64dd047863be0d7d88532ecf57311dbb9813)

#### [v1.6.17](https://github.com/emit-sds/emit-main/compare/v1.6.16...v1.6.17)

> 27 June 2023

- revise changelog [`#46`](https://github.com/emit-sds/emit-main/pull/46)
- L1BATT library issue patch update [`#45`](https://github.com/emit-sds/emit-main/pull/45)
- l2b and l3 software updates [`#44`](https://github.com/emit-sds/emit-main/pull/44)
- workflow updates to harden l1b processing with destriping [`#43`](https://github.com/emit-sds/emit-main/pull/43)
- L1b-Geo patch update (1.6.15) [`#42`](https://github.com/emit-sds/emit-main/pull/42)
- Update for latest l2b patch fix [`#41`](https://github.com/emit-sds/emit-main/pull/41)
- new update for v1.6.13 [`#40`](https://github.com/emit-sds/emit-main/pull/40)
- Merge develop into main for v1.6.12 [`#38`](https://github.com/emit-sds/emit-main/pull/38)
- Merge develop into main for v1.6.11 release [`#36`](https://github.com/emit-sds/emit-main/pull/36)
- Merge develop into main for v1.6.10 [`#35`](https://github.com/emit-sds/emit-main/pull/35)
- Merge develop into main for v1.6.9 [`#33`](https://github.com/emit-sds/emit-main/pull/33)
- Merge develop into main for v1.6.8 [`#32`](https://github.com/emit-sds/emit-main/pull/32)
- Merge develop into main for v1.6.7 [`#31`](https://github.com/emit-sds/emit-main/pull/31)
- Merge develop into main for v1.6.6 [`#29`](https://github.com/emit-sds/emit-main/pull/29)
- Merge develop into main for v1.6.5 [`#27`](https://github.com/emit-sds/emit-main/pull/27)
- Merge develop into main for v1.6.4 [`#25`](https://github.com/emit-sds/emit-main/pull/25)
- Merge develop into main for v1.6.3 [`#24`](https://github.com/emit-sds/emit-main/pull/24)
- Merge develop into main for v1.6.2 [`#23`](https://github.com/emit-sds/emit-main/pull/23)
- Merge develop into main for v1.6.1 [`#22`](https://github.com/emit-sds/emit-main/pull/22)
- Merge develop into main for v1.6.0 [`#20`](https://github.com/emit-sds/emit-main/pull/20)
- increment to version 1.6.17 [`1834f0e`](https://github.com/emit-sds/emit-main/commit/1834f0e509282f09a0c5738e5aad0fbeb975f3f2)
- small l2b updates (tar) and l3 updates (runcall and julia 1.9) [`a3657d1`](https://github.com/emit-sds/emit-main/commit/a3657d1810ea114aa1041beaa87adc02a2b339ad)
- update changelog [`4278796`](https://github.com/emit-sds/emit-main/commit/4278796d0a7ca097af3df1d070687b86739f7b4d)

#### [v1.6.16](https://github.com/emit-sds/emit-main/compare/v1.6.15...v1.6.16)

> 8 June 2023

- version number updates [`9b23b59`](https://github.com/emit-sds/emit-main/commit/9b23b597e2da4f3464b90d12f65f9a20be4d0481)
- l1b adjustment to ignore unprocessed l1a products during forward dark search [`ed9a7ff`](https://github.com/emit-sds/emit-main/commit/ed9a7ff5785d8f145ba77f4c527c4b3a462aecd8)
- update auto changelog [`f237b4f`](https://github.com/emit-sds/emit-main/commit/f237b4f4b8b973ec57da1773a7c7c4dc99d26cc1)

#### [v1.6.15](https://github.com/emit-sds/emit-main/compare/v1.6.14...v1.6.15)

> 6 June 2023

- L1b-Geo patch update (1.6.15) [`#42`](https://github.com/emit-sds/emit-main/pull/42)
- updates to incrememnt l1b-geo build [`1e5833e`](https://github.com/emit-sds/emit-main/commit/1e5833ec075bb324abcd228dabd975a2ee43ced9)
- update change log [`12ecb79`](https://github.com/emit-sds/emit-main/commit/12ecb79b493654f9505d6b3865831e2d62202cc3)

#### [v1.6.14](https://github.com/emit-sds/emit-main/compare/v1.6.13...v1.6.14)

> 1 June 2023

- Update for latest l2b patch fix [`#41`](https://github.com/emit-sds/emit-main/pull/41)
- update ops and test configs and build version [`511a6b5`](https://github.com/emit-sds/emit-main/commit/511a6b551b3575b965b35c79635810d5c913a018)
- update change log [`e75dbbf`](https://github.com/emit-sds/emit-main/commit/e75dbbf9d02c0dfdbfa7442c70ec5f94ee26bf57)
- increment to version 1.6.14 [`889c27c`](https://github.com/emit-sds/emit-main/commit/889c27c643cd1d2d81b41c635d191d6505011368)

#### [v1.6.13](https://github.com/emit-sds/emit-main/compare/v1.6.12...v1.6.13)

> 19 May 2023

- new update for v1.6.13 [`#40`](https://github.com/emit-sds/emit-main/pull/40)
- L2b updates [`#39`](https://github.com/emit-sds/emit-main/pull/39)
- Add build 010613 that uses emit-isofit conda env for l2b. Pass emit_utils to l2b run commands. [`d79241c`](https://github.com/emit-sds/emit-main/commit/d79241c272aab8d6ae1b502825a9e761b12aaf3b)
- Update abununcert hdr file with emit properties [`ffa197a`](https://github.com/emit-sds/emit-main/commit/ffa197ad82970258c4a5b6cc726532a377f9a503)
- update change log [`bc38521`](https://github.com/emit-sds/emit-main/commit/bc385210a37c230d62e9f9320c3d9c85de1d6a13)

#### [v1.6.12](https://github.com/emit-sds/emit-main/compare/v1.6.11...v1.6.12)

> 16 May 2023

- Merge develop into main for v1.6.12 [`#38`](https://github.com/emit-sds/emit-main/pull/38)
- Daily flat field and many updates to monitors and scripts [`#37`](https://github.com/emit-sds/emit-main/pull/37)
- Call new rdn wrapper script that executes flat field update and median [`e4c1e8e`](https://github.com/emit-sds/emit-main/commit/e4c1e8ea80909949951dfbba151f41edd9313a42)
- Don't use singleton. Don't ingest 1675 in monitor, use new script. Retry L2B in reconciliation. [`46a319f`](https://github.com/emit-sds/emit-main/commit/46a319f1763fe62141ee5e368c2202825ce72eb4)
- Add l2b and l3 monitors. Also, add mask dependency in l2 monitor. [`2ee9fd8`](https://github.com/emit-sds/emit-main/commit/2ee9fd838c014ad0fda0422c31d8a0bbd95f9c5b)

#### [v1.6.11](https://github.com/emit-sds/emit-main/compare/v1.6.10...v1.6.11)

> 26 April 2023

- Merge develop into main for v1.6.11 release [`#36`](https://github.com/emit-sds/emit-main/pull/36)
- Various patches and updates to reporting scripts. [`0beba72`](https://github.com/emit-sds/emit-main/commit/0beba7229ff6a655e76288c64706c5dded8f222c)
- Add status report option and fixes for pp ingest and science ingest. [`3def4df`](https://github.com/emit-sds/emit-main/commit/3def4df85b7f9ef66f1b5aea3029c21cf01b08f2)
- Prep for 1.6.11 release [`1d11b56`](https://github.com/emit-sds/emit-main/commit/1d11b565aaf2b767e1643314941bcbdbaf2961c3)

#### [v1.6.10](https://github.com/emit-sds/emit-main/compare/v1.6.9...v1.6.10)

> 17 February 2023

- Merge develop into main for v1.6.10 [`#35`](https://github.com/emit-sds/emit-main/pull/35)
- Track build and delivery versions separately in delivered products [`#34`](https://github.com/emit-sds/emit-main/pull/34)
- Add new Python ingest science data script [`1a6d1cf`](https://github.com/emit-sds/emit-main/commit/1a6d1cf9f4c58334be122f1128dab67c3c6b1e4d)
- Prep for 1.6.10 release [`ebb7f13`](https://github.com/emit-sds/emit-main/commit/ebb7f1352f0b68387cbc695705a479bb47285833)
- Track build and delivery versions separately in delivered products. [`ae95dee`](https://github.com/emit-sds/emit-main/commit/ae95deed2b1ca6e50ef64fdd75a5017728493c3b)

#### [v1.6.9](https://github.com/emit-sds/emit-main/compare/v1.6.8...v1.6.9)

> 25 January 2023

- Merge develop into main for v1.6.9 [`#33`](https://github.com/emit-sds/emit-main/pull/33)
- Prep for v1.6.9 [`75c60cd`](https://github.com/emit-sds/emit-main/commit/75c60cd11ab2e7c7e9b4f02256757ca4f9204356)
- Require all frames for reassembly. Remove milliseconds on ingest again. [`9822e2e`](https://github.com/emit-sds/emit-main/commit/9822e2eafe8cd5109b643b11c21cd4777401d4a7)
- Update change log [`581ac8c`](https://github.com/emit-sds/emit-main/commit/581ac8cbfee1e4a03012e9ed7b60256c4ba66150)

#### [v1.6.8](https://github.com/emit-sds/emit-main/compare/v1.6.7...v1.6.8)

> 12 January 2023

- Merge develop into main for v1.6.8 [`#32`](https://github.com/emit-sds/emit-main/pull/32)
- Prep for 1.6.8 [`4294d65`](https://github.com/emit-sds/emit-main/commit/4294d65136975a4cac73c297b4a86ebb0049f782)
- Update geo to 1.6.7 and support two PNGs (proj vs not) [`6d9dea8`](https://github.com/emit-sds/emit-main/commit/6d9dea8b0c82c86f51537bcd25b467cbc05da297)
- Update change log [`a5af45a`](https://github.com/emit-sds/emit-main/commit/a5af45a24b464ef33b6a7dda0e8fae17ee0c536c)

#### [v1.6.7](https://github.com/emit-sds/emit-main/compare/v1.6.6...v1.6.7)

> 6 January 2023

- Merge develop into main for v1.6.7 [`#31`](https://github.com/emit-sds/emit-main/pull/31)
- Reconciliation report v2 [`#30`](https://github.com/emit-sds/emit-main/pull/30)
- Add new monitor to process reconciliation responses [`af17ef1`](https://github.com/emit-sds/emit-main/commit/af17ef16973b1e002166e92ab9cabe7ab92b8b8d)
- Add support for L2A deliveries and extend all L1A delivery updates to other collections. [`3d0160c`](https://github.com/emit-sds/emit-main/commit/3d0160c834d24b892cdc6cfeb6def0498a841518)
- Prep for v1.6.7 [`d8605aa`](https://github.com/emit-sds/emit-main/commit/d8605aa349b1ea4c1e2c7043d39024bb73a4dda7)

#### [v1.6.6](https://github.com/emit-sds/emit-main/compare/v1.6.5...v1.6.6)

> 15 December 2022

- Merge develop into main for v1.6.6 [`#29`](https://github.com/emit-sds/emit-main/pull/29)
- L1A delivery  updates and reconciliation report draft [`#28`](https://github.com/emit-sds/emit-main/pull/28)
- Add ReconciliationReport task. [`b0f046e`](https://github.com/emit-sds/emit-main/commit/b0f046e0a16671b74dc40c1c91a130bf300ddb44)
- Prep for 1.6.6 [`9cee802`](https://github.com/emit-sds/emit-main/commit/9cee802fe5a1050cb354f814df3dffbd4dbd8255)
- Remove dependencies on higher level processing for L1ADeliver [`e1eafb5`](https://github.com/emit-sds/emit-main/commit/e1eafb538649c69db002c0bb084f761a00ec0c47)

#### [v1.6.5](https://github.com/emit-sds/emit-main/compare/v1.6.4...v1.6.5)

> 9 December 2022

- Merge develop into main for v1.6.5 [`#27`](https://github.com/emit-sds/emit-main/pull/27)
- Ioc delivery [`#26`](https://github.com/emit-sds/emit-main/pull/26)
- Add monitors for assigning DAAC scene nums and L0 and L1A delivery. [`a85d8a4`](https://github.com/emit-sds/emit-main/commit/a85d8a4e1e6c8249569411477fa4ecb543dc1dc4)
- Add script to calculate runtimes from slurm log. [`6b1cbdf`](https://github.com/emit-sds/emit-main/commit/6b1cbdf7ba9cac164f0e8675d16bba65f39ecfc0)
- Prep for 1.6.5 [`17fc363`](https://github.com/emit-sds/emit-main/commit/17fc36362556f276e8c885940c6e0349eb315902)

#### [v1.6.4](https://github.com/emit-sds/emit-main/compare/v1.6.3...v1.6.4)

> 17 November 2022

- Merge develop into main for v1.6.4 [`#25`](https://github.com/emit-sds/emit-main/pull/25)
- Prepare for 1.6.4 release [`9865eb1`](https://github.com/emit-sds/emit-main/commit/9865eb1a55459d65d669b7a68fd5daf735bfc11b)
- Update change log [`62e2d53`](https://github.com/emit-sds/emit-main/commit/62e2d53bd63e73216fc3fbf7086d6be0c3face68)

#### [v1.6.3](https://github.com/emit-sds/emit-main/compare/v1.6.2...v1.6.3)

> 11 November 2022

- Merge develop into main for v1.6.3 [`#24`](https://github.com/emit-sds/emit-main/pull/24)
- Prepare for 1.6.3 release. [`41765a5`](https://github.com/emit-sds/emit-main/commit/41765a5f847d15e3ed89c359aa713565016ff822)
- Update change log [`c379413`](https://github.com/emit-sds/emit-main/commit/c379413a9e9a09ce2cd514a5b6a882dc890d4231)

#### [v1.6.2](https://github.com/emit-sds/emit-main/compare/v1.6.1...v1.6.2)

> 10 November 2022

- Merge develop into main for v1.6.2 [`#23`](https://github.com/emit-sds/emit-main/pull/23)
- Update 1675 ingest script and prepare for 1.6.2. [`e8cd056`](https://github.com/emit-sds/emit-main/commit/e8cd0561caca14c06e10385604aba9b4c1e12149)
- Update change log [`1acb967`](https://github.com/emit-sds/emit-main/commit/1acb96767409de279486092063496893e91d06b0)
- add bandmask argument to calibration destriping [`e04b722`](https://github.com/emit-sds/emit-main/commit/e04b7221df0596bec2cd42e8e581e8ae8049db34)

#### [v1.6.1](https://github.com/emit-sds/emit-main/compare/v1.6.0...v1.6.1)

> 7 November 2022

- Merge develop into main for v1.6.1 [`#22`](https://github.com/emit-sds/emit-main/pull/22)
- Aoe updates [`#21`](https://github.com/emit-sds/emit-main/pull/21)
- Support forward/backward DAAC queues [`d3ce6ce`](https://github.com/emit-sds/emit-main/commit/d3ce6ce476d4c5a1c58686dc5ebf13d8d852a3be)
- Add build 010601 and update test/ops configs [`b12a51a`](https://github.com/emit-sds/emit-main/commit/b12a51aae31613839b6c3cff4db3f86d716e5585)
- Add FAP script [`5cf7d62`](https://github.com/emit-sds/emit-main/commit/5cf7d6235a959b531bb695501d4143ac249a3600)

#### [v1.6.0](https://github.com/emit-sds/emit-main/compare/v1.5.2...v1.6.0)

> 17 October 2022

- Merge develop into main for v1.6.0 [`#20`](https://github.com/emit-sds/emit-main/pull/20)
- Rerun updates [`#19`](https://github.com/emit-sds/emit-main/pull/19)
- UMMG Updates [`#18`](https://github.com/emit-sds/emit-main/pull/18)
- Update ioc-ummg branch with latest develop changes [`#17`](https://github.com/emit-sds/emit-main/pull/17)
- IOC Misc Fixes [`#16`](https://github.com/emit-sds/emit-main/pull/16)
- Delete before copy  [`#15`](https://github.com/emit-sds/emit-main/pull/15)
- Merge IOC hotfixes into develop [`#14`](https://github.com/emit-sds/emit-main/pull/14)
- Refl updates [`#13`](https://github.com/emit-sds/emit-main/pull/13)
- Daily report [`#12`](https://github.com/emit-sds/emit-main/pull/12)
- Merge develop into main for v1.5.2 [`#11`](https://github.com/emit-sds/emit-main/pull/11)
- Merge develop into main for v1.5.1 [`#10`](https://github.com/emit-sds/emit-main/pull/10)
- Merge develop into main for v1.5.0 [`#8`](https://github.com/emit-sds/emit-main/pull/8)
- Merge develop to main for v1.4.0 [`#4`](https://github.com/emit-sds/emit-main/pull/4)
- Add solar zenith and azimuth and gring in L1B geo. Also, add in new DAAC Helper tasks. [`7bc4eea`](https://github.com/emit-sds/emit-main/commit/7bc4eeab46e3ed48746230674ac9d829e80971c4)
- Move scene generator to L1A [`3bc0c49`](https://github.com/emit-sds/emit-main/commit/3bc0c4980c808b7245ec56ce40e3c5d70a304f24)
- Add reassembly metrics. [`d80ed73`](https://github.com/emit-sds/emit-main/commit/d80ed73d0067e9376e9d1b2668d73eab264faa86)

#### [v1.5.2](https://github.com/emit-sds/emit-main/compare/v1.5.1...v1.5.2)

> 27 July 2022

- Prep for v1.5.2 [`2901c23`](https://github.com/emit-sds/emit-main/commit/2901c23e4593a939d9c374ac101411bd20d5bce7)
- handle merge conflict [`fa5f99c`](https://github.com/emit-sds/emit-main/commit/fa5f99c2b6576a12ccec5d25124a30a66e7bdd07)
- update configs for tetracorder5.27a and newest unmixing [`0cb3cbf`](https://github.com/emit-sds/emit-main/commit/0cb3cbf67ffa4e2e7bc1356d5ad0c68ded99e211)

#### [v1.5.1](https://github.com/emit-sds/emit-main/compare/v1.5.0...v1.5.1)

> 27 July 2022

- Merge develop into main for v1.5.1 [`#10`](https://github.com/emit-sds/emit-main/pull/10)
- Planning product ingest and orbit update [`#9`](https://github.com/emit-sds/emit-main/pull/9)
- Update planning product to delete any previous overlapping orbits or data collections. First, check if any of them have products, and if so, then abort. [`d9addc1`](https://github.com/emit-sds/emit-main/commit/d9addc1107443529d4ced20f2d40ffa54421e769)
- Update filenames to use orbit.short_oid or acquisition.short_orb, including DAAC delivery files. Use '0000000' for default orbit. [`f627d80`](https://github.com/emit-sds/emit-main/commit/f627d800cdd2e682a93306633d123402ad256fca)
- Prep for v1.5.1 [`76d39c8`](https://github.com/emit-sds/emit-main/commit/76d39c86b5f39b32c85f079f1ec6131e092a326b)

#### [v1.5.0](https://github.com/emit-sds/emit-main/compare/v1.4.0...v1.5.0)

> 22 July 2022

- Merge develop into main for v1.5.0 [`#8`](https://github.com/emit-sds/emit-main/pull/8)
- Quicklooks [`#7`](https://github.com/emit-sds/emit-main/pull/7)
- Pre ioc [`#6`](https://github.com/emit-sds/emit-main/pull/6)
- Failed monitor tasks [`#5`](https://github.com/emit-sds/emit-main/pull/5)
- Add --date_field and --retry_failed flags. The date field tells the monitor which date field to search (last_modified, start_time, etc.). The retry failed flag forces the monitor to retry a task even if it failed before. [`ac540c4`](https://github.com/emit-sds/emit-main/commit/ac540c4aefea77db5cbe88c692c75fabfbcbfd97)
- Add build 010500 and update config files. [`2b68c9e`](https://github.com/emit-sds/emit-main/commit/2b68c9e21e42c2d90ca09f470a81569d673ea70c)
- Update task sizing.  Use memory = 18000 as the minimum for all small tasks. Larger tasks are apportioned according to their need. [`00ec7f2`](https://github.com/emit-sds/emit-main/commit/00ec7f2bd1de60ceedb59f30305284ccda8cb56f)

#### [v1.4.0](https://github.com/emit-sds/emit-main/compare/v1.3.0...v1.4.0)

> 6 June 2022

- Merge develop to main for v1.4.0 [`#4`](https://github.com/emit-sds/emit-main/pull/4)
- Stress test updates [`#3`](https://github.com/emit-sds/emit-main/pull/3)
- Check number of valid lines and instrument mode [`#2`](https://github.com/emit-sds/emit-main/pull/2)
- Ummg updates [`#1`](https://github.com/emit-sds/emit-main/pull/1)
- l3 call updates [`#65`](https://github.com/emit-sds/emit-main/pull/65)
- Skip cloudy acquisitions [`#66`](https://github.com/emit-sds/emit-main/pull/66)
- Update change log [`5d9170a`](https://github.com/emit-sds/emit-main/commit/5d9170a1a0cd23076cc1c9d9b6514509af297d73)
- Add cron script to rclone backup data products. [`41485be`](https://github.com/emit-sds/emit-main/commit/41485be6fa0aef4fbbb8768ff02c9e725ed05b19)
- update unmixing call and library reference, also julia settings [`0dbe8bf`](https://github.com/emit-sds/emit-main/commit/0dbe8bfb26c4ea164e64786836e50a022bf80164)

#### [v1.3.0](https://github.com/emit-sds/emit-main/compare/v1.2.0...v1.3.0)

> 4 May 2022

- Merge develop to main for v1.3.0 [`#63`](https://github.com/emit-sds/emit-main/pull/63)
- Planning product and bad updates for long duration tests [`#62`](https://github.com/emit-sds/emit-main/pull/62)
- Add code to check for start/stop of BAD file before inserting into DB. [`ac80098`](https://github.com/emit-sds/emit-main/commit/ac800983ef52fb9f894d898d4dc60ad8acd25c87)
- Add 010300 build and update test and ops to use it. Use tvac4_config.json for l1b config template. [`611846b`](https://github.com/emit-sds/emit-main/commit/611846beff9892fd5063029f13a12549bcb9a269)
- Update sftp script to only use rpsm check on hosc files, not bad files. [`49d7e77`](https://github.com/emit-sds/emit-main/commit/49d7e770905a17df6513aa861e7ff855813743ec)

#### [v1.2.0](https://github.com/emit-sds/emit-main/compare/v1.1.0...v1.2.0)

> 18 March 2022

- Merge develop to main for v1.2.0 [`#61`](https://github.com/emit-sds/emit-main/pull/61)
- L1 edp addl prods [`#60`](https://github.com/emit-sds/emit-main/pull/60)
- Ccsds updates [`#59`](https://github.com/emit-sds/emit-main/pull/59)
- Use /beegfs/store/shared instead of /shared. [`c655e77`](https://github.com/emit-sds/emit-main/commit/c655e7766b4c6a6edc2416f15be082b71dab4cf2)
- Update L1AReformatEDP to allow for multiple engineering products. Use 1676 stream with corresponding start time as ancillary input file. Call edp script in emit-ios directory instead of using the emit-sds-l1a run script. [`9cbdd1e`](https://github.com/emit-sds/emit-main/commit/9cbdd1e7e508e15f9a1e611d7dec28ad08a2a8cf)
- Update L0 HOSC task to insert stream in DB after getting start/stop time in UTC from CCSDS packets. Rename CCSDS file using this UTC timestamp so that it matches DB start time. [`f2a7257`](https://github.com/emit-sds/emit-main/commit/f2a72576326f28297d5e801d63bb208afb144f0d)

#### [v1.1.0](https://github.com/emit-sds/emit-main/compare/v1.0.0...v1.1.0)

> 28 February 2022

- Merge develop to main for v1.1.0 [`#58`](https://github.com/emit-sds/emit-main/pull/58)
- Add build 010100 and update test/ops configs to reference it. [`785a8ae`](https://github.com/emit-sds/emit-main/commit/785a8ae264e0e908da2bdf8dbfa6eb944bc9496e)
- Update changelog. [`282e7e1`](https://github.com/emit-sds/emit-main/commit/282e7e1774c5ce9c89f779d6c6939e1bf11d2f46)
- Update version to 1.1.0 [`70cb078`](https://github.com/emit-sds/emit-main/commit/70cb078e2c116c1fbfd0b294a2daae6f350431ad)

### [v1.0.0](https://github.com/emit-sds/emit-main/compare/v0.6.0...v1.0.0)

> 9 February 2022

- Merge develop to main for 1.0.0 [`#57`](https://github.com/emit-sds/emit-main/pull/57)
- Release 3 prep v2 [`#56`](https://github.com/emit-sds/emit-main/pull/56)
- Add build 010000 and update test config to use it. [`bd76141`](https://github.com/emit-sds/emit-main/commit/bd76141ce8f51d92ae14b8b5272cca5371745b09)
- Add task dependencies for all deliver tasks. [`7a865c3`](https://github.com/emit-sds/emit-main/commit/7a865c382e754eb1b3e0f79f1816119ab19a90eb)
- Add L2BAbundance to l2 monitor and update test and ops configs for deployment. [`aa79417`](https://github.com/emit-sds/emit-main/commit/aa794170efbdd157d5b897c0598d2ce255d55283)

#### [v0.6.0](https://github.com/emit-sds/emit-main/compare/v0.5.0...v0.6.0)

> 31 January 2022

- Merge develop into main for v0.6.0 [`#55`](https://github.com/emit-sds/emit-main/pull/55)
- Release 3 prep (v1) [`#54`](https://github.com/emit-sds/emit-main/pull/54)
- Integrate geo [`#53`](https://github.com/emit-sds/emit-main/pull/53)
- Ummg file creation. [`#52`](https://github.com/emit-sds/emit-main/pull/52)
- Merge develop for release 0.5.0 [`#51`](https://github.com/emit-sds/emit-main/pull/51)
- Merge develop for v0.4 [`#42`](https://github.com/emit-sds/emit-main/pull/42)
- Merge develop for version 0.3 [`#41`](https://github.com/emit-sds/emit-main/pull/41)
- Add L2BDeliver with l2bdaac product choice. Remove umm-g from L2BFormat. [`e46bf23`](https://github.com/emit-sds/emit-main/commit/e46bf2364cbafc396403b94322acc3b1d4b2dd71)
- Add L1BAttDeliver task and update orbit class to support it. [`6cf976f`](https://github.com/emit-sds/emit-main/commit/6cf976ff87088100e1638629ff41c4daf36486ad)
- add license [`f629e77`](https://github.com/emit-sds/emit-main/commit/f629e777cd775a69d13c436b3104365d65c0e45b)

#### [v0.5.0](https://github.com/emit-sds/emit-main/compare/v0.4...v0.5.0)

> 20 January 2022

- BAD CSV Convertor, L1B Cal Integration, and DAAC Delivery Updates [`#50`](https://github.com/emit-sds/emit-main/pull/50)
- Orbit monitor [`#49`](https://github.com/emit-sds/emit-main/pull/49)
- TVAC 4 Prep and Misc Updates [`#48`](https://github.com/emit-sds/emit-main/pull/48)
- Daac interface [`#47`](https://github.com/emit-sds/emit-main/pull/47)
- Bad reformatting [`#46`](https://github.com/emit-sds/emit-main/pull/46)
- Ncdf integration [`#45`](https://github.com/emit-sds/emit-main/pull/45)
- Integrate raw line timestamps product into ReassembleRaw task [`#44`](https://github.com/emit-sds/emit-main/pull/44)
- Split acquisitions [`#43`](https://github.com/emit-sds/emit-main/pull/43)
- Add working L0IngestBAD task and not-yet working L1AReformatBAD task. Add 'bad' as new apid in db and check for .sto files when doing inserts and updates. Add support for Orbit class in workflow manager. [`a0af96e`](https://github.com/emit-sds/emit-main/commit/a0af96ee87cd5e70bee922ceef71d91bb86fb590)
- Begin integration of splitting acquisitions. Not yet complete [`72f5b80`](https://github.com/emit-sds/emit-main/commit/72f5b802bdf87dfe8e1404b61f51d2396a607d2b)
- Add Orbit class to track orbits. Overhaul L0ProcessPlanningProduct to read in JSON files that also include orbit information in addition to DCIDs. Change dcid symlink on depacketization task. [`f8e4909`](https://github.com/emit-sds/emit-main/commit/f8e49097ec71887baf293d943c8b007cf06a8a27)

#### [v0.4](https://github.com/emit-sds/emit-main/compare/v0.3...v0.4)

> 26 October 2021

- Merge develop for v0.4 [`#42`](https://github.com/emit-sds/emit-main/pull/42)
- Create build_0004 which includes v0.5.6 of emit-ios code [`314d809`](https://github.com/emit-sds/emit-main/commit/314d8091b3aed58ebedd06a3f7514cfc3c878cfd)

#### [v0.3](https://github.com/emit-sds/emit-main/compare/v0.2...v0.3)

> 25 October 2021

- Merge develop for version 0.3 [`#41`](https://github.com/emit-sds/emit-main/pull/41)
- Dcid updates [`#40`](https://github.com/emit-sds/emit-main/pull/40)
- Tvac3 prep [`#39`](https://github.com/emit-sds/emit-main/pull/39)
- L3a [`#38`](https://github.com/emit-sds/emit-main/pull/38)
- Add DataCollection class. Add data_collection queries to DatabaseManager. Modify change ownership to check that target gid is different from current. [`a28b815`](https://github.com/emit-sds/emit-main/commit/a28b81533d0aa293a7db6412d42bdb15da626eb9)
- Import WorkflowManager into other classes inline in order to use helper functions like wm.makedirs and wm.symlink. For data collection class, create two folder structures (by_dcid, and by_date) - the second one is only created when there is a start_time property (which will come from planning product). [`29142be`](https://github.com/emit-sds/emit-main/commit/29142beeec8c8680291d878ac63fe31772de17d4)
- Add data_collection to workflow manager. Update Depacketize task to use data collection objects and paths instead of acquisition. [`fa0cbbb`](https://github.com/emit-sds/emit-main/commit/fa0cbbb8f779425ff5478310c16f80e1226c9797)

#### [v0.2](https://github.com/emit-sds/emit-main/compare/v0.1...v0.2)

> 31 August 2021

- Merge develop into main for SDS Release 2 [`#37`](https://github.com/emit-sds/emit-main/pull/37)
- Update l0 calling structure and missing packet thresh [`#36`](https://github.com/emit-sds/emit-main/pull/36)
- Rename CCSDS files [`#35`](https://github.com/emit-sds/emit-main/pull/35)
- Group permissions [`#34`](https://github.com/emit-sds/emit-main/pull/34)
- Failure notification email and DB authentication [`#33`](https://github.com/emit-sds/emit-main/pull/33)
- File monitor update [`#32`](https://github.com/emit-sds/emit-main/pull/32)
- Check Frame Report [`#31`](https://github.com/emit-sds/emit-main/pull/31)
- Rawqa  [`#30`](https://github.com/emit-sds/emit-main/pull/30)
- Support L1A EDD Updates [`#29`](https://github.com/emit-sds/emit-main/pull/29)
- Misc fixes, including UTC timestamps [`#28`](https://github.com/emit-sds/emit-main/pull/28)
- Resource management updates [`#27`](https://github.com/emit-sds/emit-main/pull/27)
- Update workflow code to work with latest frame header changes [`#26`](https://github.com/emit-sds/emit-main/pull/26)
- Config updates [`#25`](https://github.com/emit-sds/emit-main/pull/25)
- Integrate l1a [`#24`](https://github.com/emit-sds/emit-main/pull/24)
- Separate build config into build specific config files to be read in by parent config file. Create Config class to read in configs and handle new functionality like date specific ancillary file paths. [`859eb17`](https://github.com/emit-sds/emit-main/commit/859eb177c7d98b520e3d0e04f85241ecbf19de4e)
- Add self.config to WorkflowManager, DatabaseManager, Acquisition, Stream, and FileMonitor to hold config fields instead of using self.__dict__.update in order to manage these properties better. Update environment.yml to use pip install for tensorflow==2.0.1 since conda install was failing locally. [`a2bb939`](https://github.com/emit-sds/emit-main/commit/a2bb93903122a9a9c7db948d929cac29ac7f74ff)
- Add L0ProcessObservationsProduct to insert DCID and other planned observation parameters into DB. Add/Update L1AReadScienceFrames as a working draft.  Include db methods to insert and update observations but this will likely be removed. [`fddbb81`](https://github.com/emit-sds/emit-main/commit/fddbb81f64a8b1a9bea8ba8eaff0edbd1903a6c7)

#### v0.1

> 26 January 2021

- Merge develop into main for Release 1 [`#23`](https://github.com/emit-sds/emit-main/pull/23)
- Integrate l2b [`#22`](https://github.com/emit-sds/emit-main/pull/22)
- Update l2a [`#21`](https://github.com/emit-sds/emit-main/pull/21)
- Update L1B [`#20`](https://github.com/emit-sds/emit-main/pull/20)
- Additional updates to merge add-pytest [`#19`](https://github.com/emit-sds/emit-main/pull/19)
- Minor tweaks to fix add-pytest merge [`#18`](https://github.com/emit-sds/emit-main/pull/18)
- Add pytest [`#17`](https://github.com/emit-sds/emit-main/pull/17)
- Misc updates [`#16`](https://github.com/emit-sds/emit-main/pull/16)
- Integrate L1B [`#15`](https://github.com/emit-sds/emit-main/pull/15)
- File monitor [`#14`](https://github.com/emit-sds/emit-main/pull/14)
- Integrate l0 and l1a engineering pges [`#6`](https://github.com/emit-sds/emit-main/pull/6)
- Task completion [`#5`](https://github.com/emit-sds/emit-main/pull/5)
- Packaging overhaul [`#4`](https://github.com/emit-sds/emit-main/pull/4)
- L2a draft [`#3`](https://github.com/emit-sds/emit-main/pull/3)
- FileManager update [`#2`](https://github.com/emit-sds/emit-main/pull/2)
- Conda envs [`#1`](https://github.com/emit-sds/emit-main/pull/1)
- Adds basic framework for running workflows including tasks separated by level, a file manager class to handle paths, and ENVITarget class, and some basic unit tests [`e17f6cd`](https://github.com/emit-sds/emit-main/commit/e17f6cd5b48c5611de902f1c57bf7e069472d6cd)
- Add l2amask task [`7e66aa0`](https://github.com/emit-sds/emit-main/commit/7e66aa0e66935999672a10b7770d1dec750cd910)
- Move ExampleTask to test_tasks.py because there were issues with loading test_luigi module by slurm_runner. Convert --config_path to abspath in run_workflow and run_file_monitor. Improve references to logging.conf so that run scripts can be called from any directory. [`0bbe8af`](https://github.com/emit-sds/emit-main/commit/0bbe8af54b1b2ed8ce69ac97040e41ced5d27dcb)
