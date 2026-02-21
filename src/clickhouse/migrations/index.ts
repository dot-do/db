import { migration as m0001 } from './0001_events'
import { migration as m0002 } from './0002_actions'
import { migration as m0003 } from './0003_versions'
import { migration as m0004 } from './0004_data'
import { migration as m0005 } from './0005_rels'
import { migration as m0006 } from './0006_search'
import { migration as m0007 } from './0007_artifacts'
import { migration as m0008 } from './0008_traces'
import { migration as m0009 } from './0009_logs'
import { migration as m0010 } from './0010_sessions'
import { migration as m0011 } from './0011_observations'
import { migration as m0012 } from './0012_scores'

export const migrations = [m0001, m0002, m0003, m0004, m0005, m0006, m0007, m0008, m0009, m0010, m0011, m0012]
