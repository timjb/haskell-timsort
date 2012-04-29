module Properties
  ( tests
  , timSortList
  ) where

import Test.Framework
import Test.QuickCheck
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Data.List (sort)
import Control.Monad.Primitive (PrimState)
import Control.Monad.ST (ST, runST)
import Data.Vector (MVector, fromList, toList, freeze, thaw)
import qualified Data.Vector.Algorithms.Tim as Tim

tests :: Test
tests = testGroup "timSort"
  [ testProperty "sorts like Data.List.sort" sortCorrect
  ]

timSortList :: [Int] -> [Int]
timSortList xs = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.sort vec
  frozen <- freeze vec
  return $ toList frozen

sortCorrect :: [Int] -> Bool
sortCorrect xs = sort xs == timSortList xs