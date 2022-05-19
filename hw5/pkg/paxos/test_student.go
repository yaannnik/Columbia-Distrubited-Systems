package paxos

import (
	"coms4113/hw5/pkg/base"
)

// ToA2RejectP1 Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	// P1 send accept
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	})

	// P2 send propose before P1 send accept
	checks = append(checks, func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.N > s2.n_p
	})

	return checks
}

// ToConsensusCase5 Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	// P2 send accept
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	})

	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide
	})

	return checks
}

// NotTerminate1 Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	// P1 propose
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 2
	})

	// P3 propose
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.ResponseCount == 1 &&
			s3.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.ResponseCount == 2 &&
			s3.proposer.SuccessCount == 2
	})

	// P1 accept
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 0
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 0
	})

	return checks
}

// NotTerminate2 Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	// P1 propose
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 2
	})

	// P3 accept
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept &&
			s3.proposer.ResponseCount == 1 &&
			s3.proposer.SuccessCount == 0
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept &&
			s3.proposer.ResponseCount == 2 &&
			s3.proposer.SuccessCount == 0
	})

	return checks
}

// NotTerminate3 Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.ResponseCount == 2 &&
			s3.proposer.SuccessCount == 2
	})

	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 0
	})

	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 0
	})

	return checks
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	// P1 propose
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.n_p == 0 &&
			s2.n_p == 0 &&
			s3.n_p == 0
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 2
	})

	// P3 propose
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s1.n_p == 2 &&
			s2.n_p == 2 &&
			s3.n_p == 2
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.ResponseCount == 1 &&
			s3.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose &&
			s3.proposer.ResponseCount == 2 &&
			s3.proposer.SuccessCount == 2
	})

	// P1 accept
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	})

	// P1 accept response
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 1 &&
			s1.proposer.SuccessCount == 0
	})
	checks = append(checks, func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept &&
			s1.proposer.ResponseCount == 2 &&
			s1.proposer.SuccessCount == 0
	})

	return checks
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	//panic("fill me in")

	checks := []func(s *base.State) bool{}

	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	})

	// P3 accept response
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept &&
			s3.proposer.ResponseCount == 1 &&
			s3.proposer.SuccessCount == 1
	})
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept &&
			s3.proposer.ResponseCount == 2 &&
			s3.proposer.SuccessCount == 2
	})

	// P3 decide
	checks = append(checks, func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide
	})

	return checks
}
