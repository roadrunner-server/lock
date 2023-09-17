package lock

//func (l *locker) internalReleaseLock(ctx context.Context, id, res string) bool {
//	l.globalMu.RLock()
//
//	r, ok := l.resources.Load(res)
//	if !ok {
//		l.globalMu.RUnlock()
//		return false
//	}
//
//	rr := r.(*resource)
//	releaseMuCh := rr.resLock.releaseMuCh
//
//	l.globalMu.RUnlock()
//	select {
//	case <-releaseMuCh:
//		l.log.Debug("acquired release mutex",
//			zap.String("resource", res),
//			zap.String("id", id))
//		return true
//	case <-ctx.Done():
//		l.log.Warn("timeout exceeded, failed to acquire a lock",
//			zap.String("resource", res),
//			zap.String("id", id))
//		return false
//	}
//}
//
//func (l *locker) internalReleaseUnLock(id, res string) {
//	l.globalMu.RLock()
//
//	r, ok := l.resources.Load(res)
//	if !ok {
//		l.globalMu.RUnlock()
//		return
//	}
//
//	rr := r.(*resource)
//	l.globalMu.RUnlock()
//
//	select {
//	case rr.resLock.releaseMuCh <- struct{}{}:
//		l.log.Debug("releaseMuCh lock returned",
//			zap.String("resource", res),
//			zap.String("id", id))
//	default:
//		l.log.Debug("releaseMuCh lock not returned, channel is full",
//			zap.String("resource", res),
//			zap.String("id", id))
//		break
//	}
//}

//func (l *locker) resourceLock(ctx context.Context, id, res string) (bool, chan struct{}, chan struct{}) {
//	// only 1 goroutine might be passed here at the time
//	// lock with timeout
//
//	l.globalMu.RLock()
//
//	r, ok := l.resources.Load(res)
//	if !ok {
//		return false, nil, nil
//	}
//
//	rr := r.(*resource)
//	muCh := rr.resLock.muCh
//	releaseMuCh := rr.resLock.releaseMuCh
//
//	l.globalMu.RUnlock()
//
//	select {
//	case <-muCh:
//		// hold release mutex as well
//		select {
//		case <-releaseMuCh:
//			l.log.Debug("acquired muCh and releaseMuCh mutexes",
//				zap.String("resource", res),
//				zap.String("id", id))
//		case <-ctx.Done():
//			l.log.Warn("timeout exceeded, failed to acquire releaseMuCh",
//				zap.String("resource", res),
//				zap.String("id", id))
//			// we should return previously acquired lock mutex
//			select {
//			case muCh <- struct{}{}:
//				l.log.Debug("muCh lock returned",
//					zap.String("resource", res),
//					zap.String("id", id))
//			default:
//				l.log.Debug("muCh lock is not returned, channel is full",
//					zap.String("resource", res),
//					zap.String("id", id))
//			}
//
//			return false, nil, nil
//		}
//	case <-ctx.Done():
//		l.log.Warn("timeout exceeded, failed to acquire a lock",
//			zap.String("resource", res),
//			zap.String("id", id))
//		return false, nil, nil
//	}
//
//	return true, muCh, releaseMuCh
//}
//
//func (l *locker) resourceUnLock(id, res string) {
//	l.globalMu.RLock()
//	defer l.globalMu.RUnlock()
//
//	r, ok := l.resources.Load(res)
//	if !ok {
//		return
//	}
//
//	rr := r.(*resource)
//
//	select {
//	case rr.resLock.releaseMuCh <- struct{}{}:
//		l.log.Debug("releaseMuCh lock returned",
//			zap.String("resource", res),
//			zap.String("id", id))
//	default:
//		l.log.Debug("releaseMuCh lock not returned, channel is full",
//			zap.String("resource", res),
//			zap.String("id", id))
//	}
//
//	select {
//	case rr.resLock.muCh <- struct{}{}:
//		l.log.Debug("MuCh lock returned",
//			zap.String("resource", res),
//			zap.String("id", id))
//	default:
//		l.log.Debug("MuCh lock not returned, channel is full",
//			zap.String("resource", res),
//			zap.String("id", id))
//	}
//}

//func (l *locker) stop(ctx context.Context) {
//	l.log.Debug("received stop signal, acquiring lock/release mutexes")
//
//	ok, muCh, releaseMuCh := l.resourceLock(ctx, "stop-internal", "stop-internal")
//	if !ok {
//		panic("failed to acquire stop mutex")
//		_ = muCh
//		_ = releaseMuCh
//	}
//
//	l.log.Debug("acquired stop mutex")
//
//	// release all mutexes
//	l.resources.Range(func(key, value any) bool {
//		k := key.(string)
//		v := value.(*resource)
//
//		close(v.stopCh)
//		l.log.Debug("closed broadcast channed",
//			zap.String("resource", ""),
//			zap.String("id", k))
//		return true
//	})
//
//	l.resourceUnLock("internal-stop", "internal-stop")
//
//	l.log.Debug("signal sent to all resources")
//}
