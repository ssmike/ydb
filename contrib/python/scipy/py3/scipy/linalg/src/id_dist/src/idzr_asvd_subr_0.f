        subroutine idzr_asvd(m,n,a,krank,w,u,v,s,ier)
c
c       constructs a rank-krank SVD  u diag(s) v^*  approximating a,
c       where u is an m x krank matrix whose columns are orthonormal,
c       v is an n x krank matrix whose columns are orthonormal,
c       and diag(s) is a diagonal krank x krank matrix whose entries
c       are all nonnegative. This routine uses a randomized algorithm.
c
c       input:
c       m -- number of rows in a
c       n -- number of columns in a
c       a -- matrix to be decomposed; the present routine does not
c            alter a
c       krank -- rank of the SVD being constructed
c       w -- initialization array that routine idzr_aidi
c            has constructed (for use in the present routine,
c            w must be at least
c            (2*krank+22)*m+(6*krank+21)*n+8*krank**2+10*krank+90
c            complex*16 elements long)
c
c       output:
c       u -- matrix of orthonormal left singular vectors of a
c       v -- matrix of orthonormal right singular vectors of a
c       s -- array of singular values of a
c       ier -- 0 when the routine terminates successfully;
c              nonzero otherwise
c
c       _N.B._: The algorithm used by this routine is randomized.
c
        implicit none
        integer m,n,krank,lw,ilist,llist,iproj,lproj,icol,lcol,
     1          iwork,lwork,iwinit,lwinit,ier
        real*8 s(krank)
        complex*16 a(m,n),u(m,krank),v(n,krank),
     1             w((2*krank+22)*m+(6*krank+21)*n+8*krank**2
     2              +10*krank+90)
c
c
c       Allocate memory in w.
c
        lw = 0
c
        iwinit = lw+1
        lwinit = (2*krank+17)*n+21*m+80
        lw = lw+lwinit
c
        ilist = lw+1
        llist = n
        lw = lw+llist
c
        iproj = lw+1
        lproj = krank*(n-krank)
        lw = lw+lproj
c
        icol = lw+1
        lcol = m*krank
        lw = lw+lcol
c
        iwork = lw+1
        lwork = (krank+1)*(m+3*n+10)+9*krank**2
        lw = lw+lwork
c
c
        call idzr_asvd0(m,n,a,krank,w(iwinit),u,v,s,ier,
     1                  w(ilist),w(iproj),w(icol),w(iwork))
c
c
        return
        end
c
c
c
c
