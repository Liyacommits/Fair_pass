package com.liyacommits.fair_pass.repository;


import com.liyacommits.fair_pass.entity.Venue;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface VenueRepository extends JpaRepository<Venue, Long> {

}
