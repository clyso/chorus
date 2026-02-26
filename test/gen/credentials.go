// Copyright 2026 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gen

const (
	CEC2CredsType = "ec2"
)

func GenerateEC2AccessKeyCharacters() []rune {
	runes := []rune{}
	for i := 'A'; i <= 'Z'; i++ {
		runes = append(runes, i)
	}
	for i := '0'; i <= '9'; i++ {
		runes = append(runes, i)
	}
	return runes
}

func GenerateEC2SecretKeyCharacters() []rune {
	runes := []rune{}
	for i := 'a'; i <= 'z'; i++ {
		runes = append(runes, i)
	}
	for i := 'A'; i <= 'Z'; i++ {
		runes = append(runes, i)
	}
	for i := '0'; i <= '9'; i++ {
		runes = append(runes, i)
	}
	for _, ch := range "/+" {
		runes = append(runes, ch)
	}
	return runes
}

func GenerateUsernameCharacters() []rune {
	runes := []rune{}
	for i := 'a'; i <= 'z'; i++ {
		runes = append(runes, i)
	}
	for i := 'A'; i <= 'Z'; i++ {
		runes = append(runes, i)
	}
	for i := '0'; i <= '9'; i++ {
		runes = append(runes, i)
	}
	return runes
}

func GenerateBucketNameCharacters() []rune {
	runes := []rune{}
	for i := 'a'; i <= 'z'; i++ {
		runes = append(runes, i)
	}
	for i := '0'; i <= '9'; i++ {
		runes = append(runes, i)
	}
	return runes
}

func GeneratePasswordCharacters() []rune {
	return GenerateCommonSafeCharacters()
}

type EC2Credentials struct {
	Access string `json:"access"`
	Secret string `json:"secret"`
}

type EC2CredentialsGenerator struct {
	AccessKeyAlphabet []rune
	SecretKeyAlphabet []rune
}

func NewEC2CredentialsGenerator() *EC2CredentialsGenerator {
	return &EC2CredentialsGenerator{
		AccessKeyAlphabet: GenerateEC2AccessKeyCharacters(),
		SecretKeyAlphabet: GenerateEC2SecretKeyCharacters(),
	}
}

func (r *EC2CredentialsGenerator) Generate(rnd *Rnd) *EC2Credentials {
	return &EC2Credentials{
		Access: rnd.StringFromRunes(r.AccessKeyAlphabet, 20),
		Secret: rnd.StringFromRunes(r.SecretKeyAlphabet, 40),
	}
}

type UsernamePassword struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type UsernamePasswordGenerator struct {
	UsernameAlphabet []rune
	PasswordAlphabet []rune
}

func NewUsernamePasswordGenerator() *UsernamePasswordGenerator {
	return &UsernamePasswordGenerator{
		UsernameAlphabet: GenerateUsernameCharacters(),
		PasswordAlphabet: GeneratePasswordCharacters(),
	}
}

func (r *UsernamePasswordGenerator) Generate(rnd *Rnd) *UsernamePassword {
	return &UsernamePassword{
		Username: rnd.VarLengthStringFromRunes(r.UsernameAlphabet, 8, 16),
		Password: rnd.VarLengthStringFromRunes(r.PasswordAlphabet, 8, 20),
	}
}

type KeystoneNameGenerator struct {
	NameAlphabet []rune
}

func NewKeystoneNameGenerator() *KeystoneNameGenerator {
	return &KeystoneNameGenerator{
		NameAlphabet: GenerateUsernameCharacters(),
	}
}

func (r *KeystoneNameGenerator) Generate(rnd *Rnd) string {
	return rnd.VarLengthStringFromRunes(r.NameAlphabet, 8, 16)
}

type BucketNameGenerator struct {
	NameAlphabet []rune
}

func NewBucketNameGenerator() *BucketNameGenerator {
	return &BucketNameGenerator{
		NameAlphabet: GenerateBucketNameCharacters(),
	}
}

// Rules are more complex https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
// Approaching with easy and safe option
func (r *BucketNameGenerator) Generate(rnd *Rnd) string {
	return rnd.VarLengthStringFromRunes(r.NameAlphabet, 3, 63)
}

type CredentialsGenerator struct {
	ec2CredentialsGenerator   *EC2CredentialsGenerator
	usernamePasswordGenerator *UsernamePasswordGenerator
	keystoneNameGenerator     *KeystoneNameGenerator
	bucketNameGenerator       *BucketNameGenerator

	rnd *Rnd
}

func NewCredentialsGenerator(rnd *Rnd) *CredentialsGenerator {
	return &CredentialsGenerator{
		ec2CredentialsGenerator:   NewEC2CredentialsGenerator(),
		usernamePasswordGenerator: NewUsernamePasswordGenerator(),
		keystoneNameGenerator:     NewKeystoneNameGenerator(),
		bucketNameGenerator:       NewBucketNameGenerator(),
		rnd:                       rnd,
	}
}

func (r *CredentialsGenerator) GenerateEC2Credentials() *EC2Credentials {
	return r.ec2CredentialsGenerator.Generate(r.rnd)
}

func (r *CredentialsGenerator) GenerateUsernamePassword() *UsernamePassword {
	return r.usernamePasswordGenerator.Generate(r.rnd)
}

func (r *CredentialsGenerator) GenerateKeystoneName() string {
	return r.keystoneNameGenerator.Generate(r.rnd)
}

func (r *CredentialsGenerator) GenerateBucketName() string {
	return r.bucketNameGenerator.Generate(r.rnd)
}
